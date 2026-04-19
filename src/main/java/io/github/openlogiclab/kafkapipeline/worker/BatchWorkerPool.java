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
import io.github.openlogiclab.kafkapipeline.RecordSize;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.handler.BatchRecordHandler;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Batch processing mode. Each per-partition batch from a single poll is submitted as one task to a
 * thread pool. The entire batch shares retry/DLQ/fallback semantics.
 */
public final class BatchWorkerPool<K, V> extends WorkerPool<K, V> {

  private static final System.Logger logger = System.getLogger(BatchWorkerPool.class.getName());

  private final BatchRecordHandler<K, V> handler;
  private final RetryExecutor<K, V> retryExecutor;
  private final OffsetTracker offsetTracker;
  private final InFlightCounter counter;

  /**
   * @param concurrency number of batch processing threads
   * @param threadMode platform or virtual threads
   * @param handler user-provided batch handler
   * @param retryExecutor retry/DLQ logic
   * @param offsetTracker offset tracking
   * @param counter in-flight counter
   * @param taskQueueCapacity max queued batch tasks for PLATFORM mode; acts as a safety net when
   *     backpressure alone is insufficient. For VIRTUAL threads this is ignored (each task gets its
   *     own lightweight thread).
   */
  public BatchWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      BatchRecordHandler<K, V> handler,
      RetryExecutor<K, V> retryExecutor,
      OffsetTracker offsetTracker,
      InFlightCounter counter,
      int taskQueueCapacity) {
    super(concurrency, threadMode, "kafka-pipeline-batch-", taskQueueCapacity);
    this.handler = handler;
    this.retryExecutor = retryExecutor;
    this.offsetTracker = offsetTracker;
    this.counter = counter;
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
  public void dispatch(ConsumerRecords<K, V> records) {
    for (TopicPartition tp : records.partitions()) {
      List<ConsumerRecord<K, V>> batch = records.records(tp);
      long firstOffset = batch.getFirst().offset();
      long lastOffset = batch.getLast().offset();

      offsetTracker.registerBatch(tp, firstOffset, lastOffset);
      long batchBytes = 0;
      for (ConsumerRecord<K, V> record : batch) {
        batchBytes += RecordSize.estimateBytes(record);
      }
      counter.registered(batch.size(), batchBytes);

      long totalBytes = batchBytes;
      executor.submit(() -> processBatch(tp, batch, firstOffset, lastOffset, totalBytes));
    }
  }

  private void processBatch(
      TopicPartition tp,
      List<ConsumerRecord<K, V>> batch,
      long firstOffset,
      long lastOffset,
      long totalBytes) {
    try {
      offsetTracker.markBatchInProgress(tp, firstOffset, lastOffset);
    } catch (IllegalStateException e) {
      logger.log(
          System.Logger.Level.WARNING,
          "Cannot mark batch [{0}..{1}] in-progress for {2}: {3}",
          firstOffset,
          lastOffset,
          tp,
          e.getMessage());
      counter.completed(batch.size(), totalBytes);
      return;
    }

    String desc = "batch [" + firstOffset + ".." + lastOffset + "]";

    Exception lastError =
        retryExecutor.executeWithRetries(attempt -> handler.handleBatch(tp, batch), tp, desc);

    if (lastError == null) {
      offsetTracker.ackBatch(tp, firstOffset, lastOffset);
      counter.completed(batch.size(), totalBytes);
      return;
    }

    RetryExecutor.FailureResolution resolution =
        retryExecutor.handleFailure(batch, tp, lastError, desc);

    switch (resolution) {
      case DLQ_SUCCESS, SKIP -> offsetTracker.ackBatch(tp, firstOffset, lastOffset);
      case FAIL_PARTITION -> offsetTracker.fail(tp, firstOffset);
    }
    counter.completed(batch.size(), totalBytes);
  }
}
