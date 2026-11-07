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

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.ProcessingOrder;
import io.github.openlogiclab.kafkapipeline.RecordSize;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.handler.BatchRecordHandler;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.WindowFullException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
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
  private final OffsetTracker offsetTracker;
  private final InFlightCounter counter;
  private final PipelineMetricsCollector metricsCollector;

  // Virtual-thread executors start a thread per task with no upper bound, so concurrency is
  // enforced here instead. Null for platform threads, where the fixed-size pool already bounds it.
  private final Semaphore handlerPermits;
  // Ordered modes: partitions with a batch submitted and not yet finished.
  private final boolean ordered;
  private final Set<TopicPartition> batchInFlight = ConcurrentHashMap.newKeySet();

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
      OffsetTracker offsetTracker,
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
      OffsetTracker offsetTracker,
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
    this.handlerPermits = threadMode == ThreadMode.VIRTUAL ? new Semaphore(concurrency) : null;
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
      List<ConsumerRecord<K, V>> batch = records.records(tp);

      // Extract actual offsets from batch - don't assume consecutive offsets
      long[] offsets = new long[batch.size()];
      long batchBytes = 0;
      for (int i = 0; i < batch.size(); i++) {
        ConsumerRecord<K, V> record = batch.get(i);
        offsets[i] = record.offset();
        batchBytes += RecordSize.estimateBytes(record);
      }

      if (ordered && batchInFlight.contains(tp)) {
        // Keep partition order: this batch waits until the previous one of the partition is done.
        if (rejected.isEmpty()) rejected = new HashMap<>();
        rejected.put(tp, offsets[0]);
        continue;
      }
      if (!hasTaskQueueRoom()) {
        // Never block the poll thread on a full task queue: fetch the batch again once it drains.
        reportTaskQueueFull(tp);
        if (rejected.isEmpty()) rejected = new HashMap<>();
        rejected.put(tp, offsets[0]);
        continue;
      }
      long[] todo;
      try {
        todo = offsetTracker.registerBatchUnlessDone(tp, offsets);
      } catch (WindowFullException e) {
        if (rejected.isEmpty()) rejected = new HashMap<>();
        rejected.put(tp, offsets[0]);
        continue;
      }
      if (todo.length != offsets.length) {
        // Some records were already processed before a crash or rebalance: leave them out.
        if (todo.length == 0) continue;
        batch = withoutDone(batch, todo);
        offsets = todo;
        batchBytes = 0;
        for (ConsumerRecord<K, V> record : batch) {
          batchBytes += RecordSize.estimateBytes(record);
        }
      }
      counter.registered(batch.size(), batchBytes);

      long totalBytes = batchBytes;
      // Captured on the poll thread, where rebalance callbacks also run, so it is exactly the
      // generation the batch was registered under.
      long generation = offsetTracker.generation(tp);
      if (ordered) batchInFlight.add(tp);
      List<ConsumerRecord<K, V>> toProcess = batch;
      long[] toAck = offsets;
      executor.submit(() -> processBatch(tp, toProcess, toAck, totalBytes, generation));
    }
    return rejected;
  }

  @Override
  public boolean canResume(TopicPartition tp) {
    return taskQueueDrained() && !(ordered && batchInFlight.contains(tp));
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
    } finally {
      // Always release the in-flight slots, even if the partition was revoked mid-processing and
      // the ack below failed; otherwise backpressure would never drain.
      counter.completed(batch.size(), totalBytes);
      if (ordered) batchInFlight.remove(tp);
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
        retryExecutor.executeWithRetries(attempt -> handler.handleBatch(tp, batch), tp, desc);

    if (lastError == null) {
      ackBatch(tp, offsets, generation, desc);
      metricsCollector.recordProcessed(batch.size());
      return;
    }

    if (Thread.currentThread().isInterrupted()) {
      // Interrupted by shutdown, not a processing failure: leave the offsets unacked so they are
      // not committed and the batch is redelivered.
      logger.log(
          System.Logger.Level.DEBUG, "Interrupted while processing {0} {1}, not acking", tp, desc);
      return;
    }

    // Best-effort: DLQ or skip, always ack and continue
    RetryExecutor.FailureResolution resolution =
        retryExecutor.handleFailure(batch, tp, lastError, desc);
    if (resolution == RetryExecutor.FailureResolution.SKIPPED) {
      metricsCollector.recordFailed(batch.size());
      if (ackBatch(tp, offsets, generation, desc)) offsetTracker.markFailed(tp);
    } else {
      ackBatch(tp, offsets, generation, desc);
    }
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
