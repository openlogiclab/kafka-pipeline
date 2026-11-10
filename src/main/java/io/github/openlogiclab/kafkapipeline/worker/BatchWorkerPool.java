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
package io.github.openlogiclab.kafkapipeline.worker;

import io.github.openlogiclab.kafkapipeline.ProcessingOrder;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.handler.BatchRecordHandler;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.internal.RecordSize;
import io.github.openlogiclab.kafkapipeline.offset.ProcessingProgress;
import io.github.openlogiclab.kafkapipeline.offset.WindowFullException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.IntSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Batch processing mode. Each per-partition batch from a single poll is submitted as one task to a
 * thread pool. The entire batch shares retry/DLQ/fallback semantics.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class BatchWorkerPool<K, V> extends WorkerPool<K, V> {

  private static final System.Logger logger = System.getLogger(BatchWorkerPool.class.getName());

  private final BatchRecordHandler<K, V> handler;
  private final RetryExecutor<K, V> retryExecutor;
  private final ProcessingProgress offsetTracker;
  private final InFlightCounter counter;
  private final PipelineMetricsCollector metricsCollector;
  private final FailureResolver<K, V> failures;

  // Virtual-thread executors start a thread per task with no upper bound, so concurrency is
  // enforced here instead. Null for platform threads, where the fixed-size pool already bounds it.
  private final Semaphore handlerPermits;
  // Ordered modes allow one batch per partition in flight. Unordered mode allows up to the
  // per-partition limit (0 = unlimited), so one slow partition cannot fill the shared in-flight
  // budget and throttle every partition.
  private final boolean ordered;
  private volatile IntSupplier partitionLimit = () -> 0;
  // Batches in flight per partition. Updated only with atomic per-key compute operations, so an
  // increment can never land on an entry that a concurrent decrement just removed.
  private final ConcurrentHashMap<TopicPartition, Integer> batchesInFlight =
      new ConcurrentHashMap<>();

  /**
   * Creates a new batch worker pool.
   *
   * @param concurrency number of batch processing threads
   * @param threadMode platform or virtual threads
   * @param handler user-provided batch handler
   * @param retryExecutor retry/DLQ logic
   * @param offsetTracker offset tracking
   * @param counter in-flight counter
   * @param taskQueueCapacity max queued batch tasks for PLATFORM mode; acts as a safety net when
   *     backpressure alone is insufficient. For VIRTUAL threads this is ignored (each task gets its
   *     own lightweight thread).
   * @param metricsCollector pipeline metrics collector
   */
  public BatchWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      BatchRecordHandler<K, V> handler,
      RetryExecutor<K, V> retryExecutor,
      ProcessingProgress offsetTracker,
      InFlightCounter counter,
      int taskQueueCapacity,
      PipelineMetricsCollector metricsCollector) {
    this(
        concurrency,
        threadMode,
        handler,
        retryExecutor,
        offsetTracker,
        counter,
        taskQueueCapacity,
        metricsCollector,
        ProcessingOrder.UNORDERED);
  }

  /**
   * Creates a batch worker pool with an ordering guarantee. With {@link ProcessingOrder#KEY} or
   * {@link ProcessingOrder#PARTITION}, at most one batch per partition is in flight: records
   * fetched for a partition whose previous batch is still running are fetched again once it
   * finishes.
   *
   * @param concurrency number of worker threads
   * @param threadMode platform or virtual threads
   * @param handler the batch handler
   * @param retryExecutor retry executor
   * @param offsetTracker offset tracker
   * @param counter in-flight counter
   * @param taskQueueCapacity executor task queue capacity (platform threads)
   * @param metricsCollector metrics collector
   * @param order ordering guarantee
   */
  public BatchWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      BatchRecordHandler<K, V> handler,
      RetryExecutor<K, V> retryExecutor,
      ProcessingProgress offsetTracker,
      InFlightCounter counter,
      int taskQueueCapacity,
      PipelineMetricsCollector metricsCollector,
      ProcessingOrder order) {
    super(concurrency, threadMode, "kafka-pipeline-batch-", taskQueueCapacity);
    this.ordered = order != ProcessingOrder.UNORDERED;
    this.handler = handler;
    this.retryExecutor = retryExecutor;
    this.offsetTracker = offsetTracker;
    this.counter = counter;
    this.metricsCollector = metricsCollector;
    this.failures = new FailureResolver<>(retryExecutor, offsetTracker, metricsCollector);
    this.handlerPermits = threadMode == ThreadMode.VIRTUAL ? new Semaphore(concurrency) : null;
  }

  /**
   * Sets the per-partition limit on batches in flight in unordered mode, such as {@code
   * RecordDispatcher::effectivePartitionLimit} (the configured {@code maxConcurrencyPerPartition}
   * with its fair-share floor). Zero means unlimited. Ordered modes always allow one.
   *
   * @param limit supplies the current limit
   */
  public void partitionLimit(IntSupplier limit) {
    this.partitionLimit = limit;
  }

  private int inFlight(TopicPartition tp) {
    return batchesInFlight.getOrDefault(tp, 0);
  }

  /** Whether the partition already has as many batches in flight as it may. */
  private boolean atLimit(TopicPartition tp) {
    int limit = ordered ? 1 : partitionLimit.getAsInt();
    return limit > 0 && inFlight(tp) >= limit;
  }

  /**
   * Keeps a partition paused while it is at its batch limit, so its records are not fetched only to
   * be sent back and fetched again.
   */
  @Override
  public boolean holdsPartition(TopicPartition tp) {
    return atLimit(tp);
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("BatchWorkerPool already started");
    }
    logger.log(
        System.Logger.Level.INFO,
        "BatchWorkerPool started with {0} concurrency ({1} threads)",
        concurrency,
        threadMode);
  }

  @Override
  public Map<TopicPartition, Long> dispatch(ConsumerRecords<K, V> records) {
    Map<TopicPartition, Long> rejected = Map.of();
    for (TopicPartition tp : records.partitions()) {
      List<ConsumerRecord<K, V>> polled = records.records(tp);
      // Offsets are not assumed consecutive (compaction, transaction markers).
      long[] offsets = polled.stream().mapToLong(ConsumerRecord::offset).toArray();

      if (atLimit(tp)) {
        // Ordered: keep partition order, this batch waits until the previous one is done.
        // Unordered: the partition already has its share of batches in flight.
        rejected = reject(rejected, tp, offsets[0]);
        continue;
      }
      if (!hasTaskQueueRoom()) {
        // Never block the poll thread on a full task queue: fetch the batch again once it drains.
        reportTaskQueueFull(tp);
        rejected = reject(rejected, tp, offsets[0]);
        continue;
      }
      long[] todo;
      try {
        todo = offsetTracker.registerBatchUnlessDone(tp, offsets);
      } catch (WindowFullException e) {
        rejected = reject(rejected, tp, offsets[0]);
        continue;
      } catch (RuntimeException e) {
        // Registration is all-or-nothing, so the whole batch can be fetched again. Do not let one
        // partition's failure drop the other partitions' records from this poll.
        reportDispatchFailure(tp, offsets[0], e);
        rejected = reject(rejected, tp, offsets[0]);
        continue;
      }
      // Records already processed before a crash or rebalance are left out.
      if (todo.length == 0) continue;
      List<ConsumerRecord<K, V>> batch =
          todo.length == offsets.length ? polled : withoutDone(polled, todo);
      submit(tp, batch, todo);
    }
    return rejected;
  }

  private void submit(TopicPartition tp, List<ConsumerRecord<K, V>> batch, long[] offsets) {
    long bytes = RecordSize.estimateBytes(batch);
    counter.registered(batch.size(), bytes);
    // Captured on the poll thread, where rebalance callbacks also run, so it is exactly the
    // generation the batch was registered under.
    long generation = offsetTracker.generation(tp);
    batchesInFlight.merge(tp, 1, Integer::sum);
    executor.submit(() -> processBatch(tp, batch, offsets, bytes, generation));
  }

  @Override
  public boolean canResume(TopicPartition tp) {
    return taskQueueDrained() && !atLimit(tp);
  }

  private void processBatch(
      TopicPartition tp,
      List<ConsumerRecord<K, V>> batch,
      long[] offsets,
      long totalBytes,
      long generation) {
    try {
      if (handlerPermits == null) {
        process(tp, batch, offsets, generation);
      } else {
        handlerPermits.acquire();
        try {
          if (!running.get()) {
            // Stopped while this batch waited: a batch that held the permit can release it before
            // the shutdown interrupt reaches this thread. Not started, so leave it for redelivery.
            return;
          }
          process(tp, batch, offsets, generation);
        } finally {
          handlerPermits.release();
        }
      }
    } catch (InterruptedException e) {
      // Shutdown while waiting for a permit: the batch was never started, so leave its offsets
      // unacked and let it be redelivered.
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      // submit() would hide this in an unread Future; log it (and stop on a fatal error).
      reportWorkerError(t);
    } finally {
      // Always release the in-flight slots, even if the partition was revoked mid-processing and
      // the ack below failed; otherwise backpressure would never drain.
      counter.completed(batch.size(), totalBytes);
      batchesInFlight.computeIfPresent(tp, (x, n) -> n <= 1 ? null : n - 1);
    }
  }

  private void process(
      TopicPartition tp, List<ConsumerRecord<K, V>> batch, long[] offsets, long generation) {
    long firstOffset = offsets[0];
    long lastOffset = offsets[offsets.length - 1];

    try {
      offsetTracker.markBatchInProgress(tp, offsets, generation);
    } catch (IllegalStateException e) {
      logger.log(
          System.Logger.Level.WARNING,
          "Cannot mark batch [{0}..{1}] in-progress for {2}: {3}",
          firstOffset,
          lastOffset,
          tp,
          e.getMessage());
      return;
    }

    String desc = "batch [" + firstOffset + ".." + lastOffset + "]";

    Exception lastError =
        retryExecutor.executeWithRetries(
            attempt -> handler.handleBatch(tp, batch), tp, desc, this::stopping);

    if (lastError == null) {
      ackBatch(tp, offsets, generation, desc);
      metricsCollector.recordProcessed(batch.size());
      return;
    }

    failures.resolve(batch, tp, lastError, desc, () -> ackBatch(tp, offsets, generation, desc));
  }

  /**
   * Acks a batch. Returns false if the partition is no longer tracked, which happens when a batch
   * finishes after its partition was revoked and the drain timeout expired. The new owner
   * re-consumes it from the last committed offset, so there is nothing more to do here.
   */
  private boolean ackBatch(TopicPartition tp, long[] offsets, long generation, String desc) {
    try {
      offsetTracker.ackBatch(tp, offsets, generation);
      return true;
    } catch (IllegalStateException e) {
      logger.log(
          System.Logger.Level.DEBUG,
          "Finished {0} {1} but could not ack it ({2}); the partition was likely revoked and the"
              + " batch will be redelivered to its new owner",
          tp,
          desc,
          e.getMessage());
      return false;
    }
  }

  private static <K, V> List<ConsumerRecord<K, V>> withoutDone(
      List<ConsumerRecord<K, V>> batch, long[] todo) {
    List<ConsumerRecord<K, V>> kept = new ArrayList<>(todo.length);
    int t = 0;
    for (ConsumerRecord<K, V> record : batch) {
      if (t < todo.length && record.offset() == todo[t]) {
        kept.add(record);
        t++;
      }
    }
    return kept;
  }
}
