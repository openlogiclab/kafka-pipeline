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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.RecordSize;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.WindowFullException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Per-record processing mode. Worker threads pull from {@link RecordDispatcher} queues and execute:
 *
 * <pre>
 *   markInProgress → hook.beforeProcess
 *       → handler.handle (with retry)
 *       → success: hook.afterProcess → ack
 *       → failure: DLQ → FinalFailureHandler → skip
 * </pre>
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class SingleRecordWorkerPool<K, V> extends WorkerPool<K, V> {

  private static final System.Logger logger =
      System.getLogger(SingleRecordWorkerPool.class.getName());

  private final RecordHandler<K, V> handler;
  private final ProcessingLifecycleHook<K, V> hook;
  private final RetryExecutor<K, V> retryExecutor;
  private final OffsetTracker offsetTracker;
  private final RecordDispatcher<K, V> dispatcher;
  private final InFlightCounter counter;
  private final PipelineMetricsCollector metricsCollector;

  /**
   * Creates a single-record worker pool.
   *
   * @param concurrency number of worker threads
   * @param threadMode platform or virtual threads
   * @param handler the record handler
   * @param hook the lifecycle hook
   * @param retryExecutor the retry executor
   * @param offsetTracker the offset tracker
   * @param dispatcher the record dispatcher
   * @param counter the in-flight counter
   * @param metricsCollector the metrics collector
   */
  public SingleRecordWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      RecordHandler<K, V> handler,
      ProcessingLifecycleHook<K, V> hook,
      RetryExecutor<K, V> retryExecutor,
      OffsetTracker offsetTracker,
      RecordDispatcher<K, V> dispatcher,
      InFlightCounter counter,
      PipelineMetricsCollector metricsCollector) {
    super(concurrency, threadMode, "kafka-pipeline-worker-", 0);
    this.handler = handler;
    this.hook = hook;
    this.retryExecutor = retryExecutor;
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.counter = counter;
    this.metricsCollector = metricsCollector;
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("WorkerPool already started");
    }
    for (int i = 0; i < concurrency; i++) {
      executor.submit(this::workerLoop);
    }
    logger.log(
        System.Logger.Level.INFO,
        "SingleRecordWorkerPool started with {0} workers ({1} threads)",
        concurrency,
        threadMode);
  }

  @Override
  public Map<TopicPartition, Long> dispatch(ConsumerRecords<K, V> records) {
    Map<TopicPartition, Long> rejected = Map.of();
    int dispatched = 0;
    for (TopicPartition tp : records.partitions()) {
      for (ConsumerRecord<K, V> record : records.records(tp)) {
        if (!dispatcher.hasRoom(tp)) {
          // Never block the poll thread on a full queue: fetch the rest again once it drains.
          dispatcher.reportFull(tp);
          if (rejected.isEmpty()) rejected = new HashMap<>();
          rejected.put(tp, record.offset());
          break;
        }
        try {
          if (!offsetTracker.registerUnlessDone(tp, record.offset())) {
            // Already processed before a crash or rebalance; recorded as done, not dispatched.
            continue;
          }
        } catch (WindowFullException e) {
          if (rejected.isEmpty()) rejected = new HashMap<>();
          rejected.put(tp, record.offset());
          break;
        }
        counter.registered(1, RecordSize.estimateBytes(record));
        dispatcher.dispatch(tp, record);
        dispatched++;
      }
    }
    dispatcher.wakeWorkers(dispatched);
    return rejected;
  }

  @Override
  public boolean canResume(TopicPartition tp) {
    return dispatcher.hasDrained(tp);
  }

  private void workerLoop() {
    while (running.get()) {
      try {
        if (Thread.currentThread().isInterrupted()) break;
        RecordDispatcher.PollResult<K, V> result = dispatcher.poll(100, MILLISECONDS);
        if (result == null) continue;
        try {
          processRecord(result.partition(), result.record(), result.generation());
        } finally {
          dispatcher.release(result);
        }
      } catch (Exception e) {
        logger.log(System.Logger.Level.ERROR, "Unexpected error in worker loop", e);
      }
    }
  }

  private void processRecord(TopicPartition tp, ConsumerRecord<K, V> record, long generation) {
    try {
      process(tp, record, generation);
    } finally {
      // Always release the in-flight slot, even if the partition was revoked mid-processing and
      // the ack below failed; otherwise backpressure would never drain.
      counter.completed(1, RecordSize.estimateBytes(record));
    }
  }

  private void process(TopicPartition tp, ConsumerRecord<K, V> record, long generation) {
    long offset = record.offset();
    ProcessingContext ctx = new ProcessingContext(tp, offset, 0);

    try {
      offsetTracker.markInProgress(tp, offset, generation);
    } catch (IllegalStateException e) {
      logger.log(
          System.Logger.Level.WARNING,
          "Cannot mark offset {0} in-progress for {1}: {2}",
          offset,
          tp,
          e.getMessage());
      return;
    }

    boolean shouldSkipViaHook = false;
    try {
      shouldSkipViaHook = !hook.beforeProcess(record, ctx);
    } catch (Exception e) {
      logger.log(
          System.Logger.Level.WARNING,
          "beforeProcess hook threw for {0}:{1}, proceeding with handler",
          tp,
          offset);
    }

    if (shouldSkipViaHook) {
      ack(tp, offset, generation);
      metricsCollector.recordSkipped(1);
      return;
    }

    String desc = "offset " + offset;
    int[] lastAttempt = {0};
    Exception lastError =
        retryExecutor.executeWithRetries(
            attempt -> {
              lastAttempt[0] = attempt;
              try {
                handler.handle(record);
              } catch (Exception ex) {
                hook.onError(record, new ProcessingContext(tp, offset, attempt), ex);
                throw ex;
              }
            },
            tp,
            desc);

    if (lastError == null) {
      // Outside the retry loop: a hook that throws must not re-run a handler that succeeded.
      try {
        hook.afterProcess(record, new ProcessingContext(tp, offset, lastAttempt[0]));
      } catch (Exception e) {
        logger.log(
            System.Logger.Level.WARNING,
            "afterProcess hook threw for " + tp + ":" + offset + "; the record is still acked",
            e);
      }
      ack(tp, offset, generation);
      metricsCollector.recordProcessed(1);
      return;
    }

    if (Thread.currentThread().isInterrupted()) {
      // Interrupted by shutdown, not a processing failure: leave the offset unacked so it is
      // not committed and the record is redelivered.
      logger.log(
          System.Logger.Level.DEBUG,
          "Interrupted while processing {0}:{1}, not acking",
          tp,
          offset);
      return;
    }

    // Best-effort: DLQ or skip, always ack and continue
    RetryExecutor.FailureResolution resolution =
        retryExecutor.handleFailure(List.of(record), tp, lastError, desc);
    if (resolution == RetryExecutor.FailureResolution.SKIPPED) {
      metricsCollector.recordFailed(1);
      if (ack(tp, offset, generation)) offsetTracker.markFailed(tp);
    } else {
      ack(tp, offset, generation);
    }
  }

  /**
   * Acks an offset. Returns false if the partition is no longer tracked, which happens when a
   * record finishes after its partition was revoked and the drain timeout expired. The new owner
   * re-consumes it from the last committed offset, so there is nothing more to do here.
   */
  private boolean ack(TopicPartition tp, long offset, long generation) {
    try {
      offsetTracker.ack(tp, offset, generation);
      return true;
    } catch (IllegalStateException e) {
      logger.log(
          System.Logger.Level.DEBUG,
          "Finished {0}:{1} but could not ack it ({2}); the partition was likely revoked and the"
              + " record will be redelivered to its new owner",
          tp,
          offset,
          e.getMessage());
      return false;
    }
  }
}
