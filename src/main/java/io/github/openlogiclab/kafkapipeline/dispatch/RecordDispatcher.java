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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Routes records from the Poller to per-partition {@link BlockingQueue}s. Workers pull from these
 * queues for processing.
 *
 * <p>Thread-safe: the Poller thread dispatches, worker threads poll, and rebalance callbacks
 * add/remove queues.
 */
public final class RecordDispatcher<K, V> {

  private static final System.Logger logger = System.getLogger(RecordDispatcher.class.getName());

  private final ConcurrentHashMap<TopicPartition, BlockingQueue<ConsumerRecord<K, V>>> queues =
      new ConcurrentHashMap<>();

  private final int queueCapacity;
  private final Set<Thread> waitingWorkers = ConcurrentHashMap.newKeySet();

  // Cached snapshot of partition entries, rebuilt only on addPartition/removePartition.
  // tryPollAll() reads this volatile reference instead of calling toArray() on every poll.
  @SuppressWarnings("unchecked")
  private volatile Map.Entry<TopicPartition, BlockingQueue<ConsumerRecord<K, V>>>[]
      partitionSnapshot = new Map.Entry[0];

  /**
   * Value returned by {@link #poll}. Carries the pre-resolved {@link TopicPartition} so callers
   * avoid allocating a new one from the record's topic/partition fields.
   */
  public record PollResult<K, V>(TopicPartition partition, ConsumerRecord<K, V> record) {}

  public RecordDispatcher(int queueCapacity) {
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be positive, got " + queueCapacity);
    }
    this.queueCapacity = queueCapacity;
  }

  public void addPartition(TopicPartition tp) {
    queues.putIfAbsent(tp, new LinkedBlockingQueue<>(queueCapacity));
    rebuildSnapshot();
    logger.log(System.Logger.Level.DEBUG, "Added dispatch queue for {0}", tp);
  }

  public List<ConsumerRecord<K, V>> removePartition(TopicPartition tp) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queues.remove(tp);
    rebuildSnapshot();
    if (queue == null) return List.of();
    List<ConsumerRecord<K, V>> drained = new ArrayList<>(queue.size());
    queue.drainTo(drained);
    logger.log(
        System.Logger.Level.DEBUG,
        "Removed dispatch queue for {0}, discarded {1} records",
        tp,
        drained.size());
    return drained;
  }

  @SuppressWarnings("unchecked")
  private void rebuildSnapshot() {
    partitionSnapshot = queues.entrySet().toArray(new Map.Entry[0]);
  }

  /**
   * Enqueues a record into the partition's dispatch queue. The caller provides the pre-built {@link
   * TopicPartition} to avoid redundant object allocation on the hot path.
   */
  public void dispatch(TopicPartition tp, ConsumerRecord<K, V> record) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queues.get(tp);
    if (queue == null) {
      throw new IllegalStateException(
          "No queue for partition " + tp + ". Was addPartition() called?");
    }
    if (!queue.offer(record)) {
      logger.log(
          System.Logger.Level.WARNING,
          "Dispatch queue full for {0}, blocking until space is available — "
              + "review backpressure thresholds vs dispatchQueueCapacity",
          tp);
      try {
        queue.put(record);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Unparks all waiting workers. Call once after dispatching a batch of records, rather than per
   * record, to avoid redundant syscalls.
   */
  public void wakeWorkers() {
    for (Thread t : waitingWorkers) {
      LockSupport.unpark(t);
    }
  }

  /**
   * Polls across all partition queues for the next available record. Returns a {@link PollResult}
   * containing both the record and its pre-resolved {@link TopicPartition}, or {@code null} if no
   * record is available within the timeout.
   */
  public PollResult<K, V> poll(long timeout, TimeUnit unit) {
    PollResult<K, V> result = tryPollAll();
    if (result != null) return result;

    waitingWorkers.add(Thread.currentThread());
    try {
      LockSupport.parkNanos(this, unit.toNanos(timeout));
    } finally {
      waitingWorkers.remove(Thread.currentThread());
    }

    return tryPollAll();
  }

  private PollResult<K, V> tryPollAll() {
    Map.Entry<TopicPartition, BlockingQueue<ConsumerRecord<K, V>>>[] snap = partitionSnapshot;
    int len = snap.length;
    if (len == 0) return null;

    int start = ThreadLocalRandom.current().nextInt(len);
    for (int i = 0; i < len; i++) {
      Map.Entry<TopicPartition, BlockingQueue<ConsumerRecord<K, V>>> entry =
          snap[(start + i) % len];
      ConsumerRecord<K, V> record = entry.getValue().poll();
      if (record != null) {
        return new PollResult<>(entry.getKey(), record);
      }
    }
    return null;
  }

  public int totalQueuedRecords() {
    int total = 0;
    for (BlockingQueue<ConsumerRecord<K, V>> queue : queues.values()) {
      total += queue.size();
    }
    return total;
  }

  public int queuedRecords(TopicPartition tp) {
    BlockingQueue<ConsumerRecord<K, V>> queue = queues.get(tp);
    return queue != null ? queue.size() : 0;
  }

  public Set<TopicPartition> partitions() {
    return Set.copyOf(queues.keySet());
  }

  public void retainOnly(Collection<TopicPartition> partitions) {
    queues.keySet().retainAll(partitions);
  }
}
