/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.openlogiclab.kafkapipeline.dispatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Routes records from the Poller to per-partition {@link BlockingQueue}s. Workers pull from these
 * queues for processing.
 *
 * <p>Thread-safe: the Poller thread dispatches, worker threads poll, and rebalance callbacks
 * add/remove queues.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public final class RecordDispatcher<K, V> {

  private static final System.Logger logger = System.getLogger(RecordDispatcher.class.getName());

  private final ConcurrentHashMap<TopicPartition, Lane<K, V>> queues = new ConcurrentHashMap<>();

  private final int queueCapacity;
  // Configured max records of one partition being processed at once; 0 means unlimited.
  private final int maxConcurrencyPerPartition;
  // Worker count, used so the limit never drops below a fair share of the pool.
  private final int concurrency;
  // Limit actually enforced: max(configured, ceil(concurrency / assigned partitions)). Recomputed
  // whenever partitions are added or removed. 0 means unlimited.
  private volatile int effectiveLimit;
  private final Set<Thread> waitingWorkers = ConcurrentHashMap.newKeySet();

  // Approximate number of queued records, used to decide whether to wake another worker. It can
  // briefly lag the queues; the park timeout in poll() bounds the cost of a missed wakeup.
  private final AtomicInteger queued = new AtomicInteger();

  // Rate limit for the "queue full" warning: at most one log line per interval.
  private static final long QUEUE_FULL_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
  private final AtomicLong lastQueueFullLogNanos =
      new AtomicLong(System.nanoTime() - QUEUE_FULL_LOG_INTERVAL_NANOS);
  private final AtomicLong queueFullBlocks = new AtomicLong();

  // Cached snapshot of partition entries, rebuilt only on addPartition/removePartition.
  // tryPollAll() reads this volatile reference instead of calling toArray() on every poll.
  @SuppressWarnings("unchecked")
  private volatile Lane<K, V>[] partitionSnapshot = new Lane[0];

  /**
   * A partition's queue plus the number of its records currently being processed. Opaque outside
   * this class; returned in {@link PollResult} so {@link #release} decrements the counter of the
   * exact queue the record came from, even if the partition was revoked and re-added meanwhile.
   *
   * @param <K> the record key type
   * @param <V> the record value type
   */
  public static final class Lane<K, V> {
    private final TopicPartition tp;
    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private final AtomicInteger active = new AtomicInteger();

    private Lane(TopicPartition tp, int capacity) {
      this.tp = tp;
      this.queue = new LinkedBlockingQueue<>(capacity);
    }
  }

  /**
   * Value returned by {@link #poll}. Carries the pre-resolved {@link TopicPartition} so callers
   * avoid allocating a new one from the record's topic/partition fields.
   *
   * @param <K> the record key type
   * @param <V> the record value type
   * @param partition the topic-partition the record belongs to
   * @param record the consumer record
   * @param lane internal handle to pass back via {@link #release}; null if no per-partition limit
   */
  public record PollResult<K, V>(
      TopicPartition partition, ConsumerRecord<K, V> record, Lane<K, V> lane) {}

  /**
   * Creates a record dispatcher.
   *
   * @param queueCapacity capacity of each per-partition queue
   */
  public RecordDispatcher(int queueCapacity) {
    this(queueCapacity, 0);
  }

  /**
   * Creates a record dispatcher that hands out at most {@code maxConcurrencyPerPartition} records
   * of the same partition at a time. Workers must call {@link #release} after processing each
   * polled record.
   *
   * @param queueCapacity capacity of each per-partition queue
   * @param maxConcurrencyPerPartition max records of one partition in processing at once; 0 means
   *     unlimited
   */
  public RecordDispatcher(int queueCapacity, int maxConcurrencyPerPartition) {
    this(queueCapacity, maxConcurrencyPerPartition, 0);
  }

  /**
   * Creates a record dispatcher with a per-partition limit that never drops below a fair share of
   * the worker pool: the enforced limit is {@code max(maxConcurrencyPerPartition, ceil(concurrency
   * / assignedPartitions))}. With a single assigned partition it can use every worker; with many,
   * none can take more than its slice.
   *
   * @param queueCapacity capacity of each per-partition queue
   * @param maxConcurrencyPerPartition max records of one partition in processing at once; 0 means
   *     unlimited
   * @param concurrency number of workers, for the fair-share floor; 0 disables the floor
   */
  public RecordDispatcher(int queueCapacity, int maxConcurrencyPerPartition, int concurrency) {
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be positive, got " + queueCapacity);
    }
    if (maxConcurrencyPerPartition < 0) {
      throw new IllegalArgumentException(
          "maxConcurrencyPerPartition must be >= 0, got " + maxConcurrencyPerPartition);
    }
    if (concurrency < 0) {
      throw new IllegalArgumentException("concurrency must be >= 0, got " + concurrency);
    }
    this.queueCapacity = queueCapacity;
    this.maxConcurrencyPerPartition = maxConcurrencyPerPartition;
    this.concurrency = concurrency;
    this.effectiveLimit = maxConcurrencyPerPartition;
  }

  /**
   * Adds a partition for dispatching.
   *
   * @param tp the partition to add
   */
  public void addPartition(TopicPartition tp) {
    queues.putIfAbsent(tp, new Lane<>(tp, queueCapacity));
    rebuildSnapshot();
    logger.log(System.Logger.Level.DEBUG, "Added dispatch queue for {0}", tp);
  }

  /**
   * Removes a partition and drains its queue.
   *
   * @param tp the partition to remove
   * @return the drained records
   */
  public List<ConsumerRecord<K, V>> removePartition(TopicPartition tp) {
    Lane<K, V> lane = queues.remove(tp);
    rebuildSnapshot();
    if (lane == null) return List.of();
    BlockingQueue<ConsumerRecord<K, V>> queue = lane.queue;
    List<ConsumerRecord<K, V>> drained = new ArrayList<>(queue.size());
    queue.drainTo(drained);
    queued.addAndGet(-drained.size());
    logger.log(
        System.Logger.Level.DEBUG,
        "Removed dispatch queue for {0}, discarded {1} records",
        tp,
        drained.size());
    return drained;
  }

  @SuppressWarnings("unchecked")
  private void rebuildSnapshot() {
    Lane<K, V>[] snap = queues.values().toArray(new Lane[0]);
    partitionSnapshot = snap;
    if (maxConcurrencyPerPartition > 0) {
      int fairShare = snap.length == 0 ? 0 : (concurrency + snap.length - 1) / snap.length;
      effectiveLimit = Math.max(maxConcurrencyPerPartition, fairShare);
    }
  }

  /**
   * Returns whether the partition's queue has room for at least one more record. The poll thread is
   * the only producer, so a {@code true} result stays true until it dispatches.
   *
   * @param tp the partition
   * @return true if {@link #dispatch} will not find the queue full
   */
  public boolean hasRoom(TopicPartition tp) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queueOf(tp);
    return queue != null && queue.remainingCapacity() > 0;
  }

  /**
   * Returns whether the partition's queue has drained to at most half its capacity, the point at
   * which a partition paused for a full queue may resume fetching.
   *
   * @param tp the partition
   * @return true if the queue is at most half full (or no longer exists)
   */
  public boolean hasDrained(TopicPartition tp) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queueOf(tp);
    return queue == null || queue.size() <= queueCapacity / 2;
  }

  /**
   * Enqueues a record into the partition's dispatch queue. Never blocks: callers must check {@link
   * #hasRoom} first. The caller provides the pre-built {@link TopicPartition} to avoid redundant
   * object allocation on the hot path.
   *
   * @param tp the partition
   * @param record the record to dispatch
   * @throws IllegalStateException if the partition has no queue or the queue is full
   */
  public void dispatch(TopicPartition tp, ConsumerRecord<K, V> record) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queueOf(tp);
    if (queue == null) {
      throw new IllegalStateException(
          "No queue for partition " + tp + ". Was addPartition() called?");
    }
    if (!queue.offer(record)) {
      throw new IllegalStateException("Dispatch queue full for " + tp + "; check hasRoom() first");
    }
    queued.incrementAndGet();
  }

  /**
   * Records that a partition's remaining records were not dispatched because its queue was full.
   * Logs a rate-limited warning: at most one line per interval, with a count.
   *
   * @param tp the partition
   */
  public void reportFull(TopicPartition tp) {
    long blocks = queueFullBlocks.incrementAndGet();
    long now = System.nanoTime();
    long last = lastQueueFullLogNanos.get();
    if (now - last >= QUEUE_FULL_LOG_INTERVAL_NANOS
        && lastQueueFullLogNanos.compareAndSet(last, now)) {
      queueFullBlocks.addAndGet(-blocks);
      logger.log(
          System.Logger.Level.WARNING,
          "Dispatch queue full for {0}, pausing the partition until workers catch up ({1} times"
              + " since last warning) — consider more concurrency or a larger"
              + " dispatchQueueCapacity",
          tp,
          blocks);
    }
  }

  /**
   * Wakes up to {@code records} waiting workers. Call once after dispatching a batch of records,
   * rather than per record, to avoid redundant syscalls. Waking more workers than there are records
   * would only make the extra ones scan the queues and park again.
   *
   * @param records number of records just dispatched
   */
  public void wakeWorkers(int records) {
    if (records <= 0) return;
    int woken = 0;
    for (Thread t : waitingWorkers) {
      LockSupport.unpark(t);
      if (++woken >= records) break;
    }
  }

  private void wakeOneWorker() {
    // The caller has already removed itself from waitingWorkers.
    for (Thread t : waitingWorkers) {
      LockSupport.unpark(t);
      return;
    }
  }

  /**
   * Polls across all partition queues for the next available record. Returns a {@link PollResult}
   * containing both the record and its pre-resolved {@link TopicPartition}, or {@code null} if no
   * record is available within the timeout.
   *
   * @param timeout the max time to wait
   * @param unit the time unit
   * @return poll result, or null if timeout
   */
  public PollResult<K, V> poll(long timeout, TimeUnit unit) {
    PollResult<K, V> result = tryPollAll();
    if (result == null) {
      Thread self = Thread.currentThread();
      waitingWorkers.add(self);
      try {
        // Re-check after registering: a record dispatched between the first check and the add
        // would otherwise not wake this worker until the timeout.
        result = tryPollAll();
        if (result == null) {
          LockSupport.parkNanos(this, unit.toNanos(timeout));
          result = tryPollAll();
        }
      } finally {
        waitingWorkers.remove(self);
      }
    }
    // If records are still queued, hand the wakeup on so they don't wait for a timeout. This
    // covers wakeups that went to a worker that found its record already taken.
    if (result != null && queued.get() > 0) {
      wakeOneWorker();
    }
    return result;
  }

  private PollResult<K, V> tryPollAll() {
    Lane<K, V>[] snap = partitionSnapshot;
    int len = snap.length;
    if (len == 0) return null;

    int limit = effectiveLimit;
    int start = ThreadLocalRandom.current().nextInt(len);
    for (int i = 0; i < len; i++) {
      Lane<K, V> lane = snap[(start + i) % len];
      if (limit == 0) {
        ConsumerRecord<K, V> record = lane.queue.poll();
        if (record != null) {
          queued.decrementAndGet();
          return new PollResult<>(lane.tp, record, null);
        }
      } else if (lane.queue.peek() != null && tryAcquire(lane, limit)) {
        ConsumerRecord<K, V> record = lane.queue.poll();
        if (record != null) {
          queued.decrementAndGet();
          return new PollResult<>(lane.tp, record, lane);
        }
        lane.active.decrementAndGet();
      }
    }
    return null;
  }

  private boolean tryAcquire(Lane<K, V> lane, int limit) {
    while (true) {
      int active = lane.active.get();
      if (active >= limit) return false;
      if (lane.active.compareAndSet(active, active + 1)) return true;
    }
  }

  /**
   * Signals that a record returned by {@link #poll} has finished processing, freeing its slot in
   * the per-partition concurrency limit. Must be called exactly once per polled record, whether
   * processing succeeded or failed. A no-op when there is no per-partition limit.
   *
   * @param result the poll result whose record finished
   */
  public void release(PollResult<K, V> result) {
    Lane<K, V> lane = result.lane();
    if (lane == null) return;
    lane.active.decrementAndGet();
    // Records of this partition may be waiting only because the partition was at its limit, so no
    // dispatch will wake a worker for them.
    if (!lane.queue.isEmpty()) {
      wakeOneWorker();
    }
  }

  /**
   * Returns the per-partition limit currently enforced, after applying the fair-share floor.
   *
   * @return records of one partition allowed in processing at once, or 0 if unlimited
   */
  public int effectivePartitionLimit() {
    return effectiveLimit;
  }

  /**
   * Returns how many records of the partition are being processed right now. Only tracked when a
   * per-partition limit is set; otherwise always 0.
   *
   * @param tp the partition
   * @return records in processing
   */
  public int activeRecords(TopicPartition tp) {
    Lane<K, V> lane = queues.get(tp);
    return lane != null ? lane.active.get() : 0;
  }

  private BlockingQueue<ConsumerRecord<K, V>> queueOf(TopicPartition tp) {
    Lane<K, V> lane = queues.get(tp);
    return lane != null ? lane.queue : null;
  }

  /**
   * Returns total queued records across all partitions.
   *
   * @return total queued count
   */
  public int totalQueuedRecords() {
    int total = 0;
    for (Lane<K, V> lane : queues.values()) {
      total += lane.queue.size();
    }
    return total;
  }

  /**
   * Returns queued records for a partition.
   *
   * @param tp the partition
   * @return queued count
   */
  public int queuedRecords(TopicPartition tp) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queueOf(tp);
    return queue != null ? queue.size() : 0;
  }

  /**
   * Returns all partitions.
   *
   * @return set of partitions
   */
  public Set<TopicPartition> partitions() {
    return Set.copyOf(queues.keySet());
  }

  /**
   * Removes all partitions not in the given set.
   *
   * @param partitions the partitions to retain
   */
  public void retainOnly(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions()) {
      if (!partitions.contains(tp)) {
        removePartition(tp);
      }
    }
  }
}
