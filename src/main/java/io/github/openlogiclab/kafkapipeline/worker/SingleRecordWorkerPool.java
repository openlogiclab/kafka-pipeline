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

import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.RecordSize;
import io.github.openlogiclab.kafkapipeline.internal.WorkerMetrics;
import io.github.openlogiclab.kafkapipeline.offset.ProcessingProgress;
import io.github.openlogiclab.kafkapipeline.offset.WindowFullException;
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
  private final ProcessingProgress offsetTracker;
  private final RecordDispatcher<K, V> dispatcher;
  private final InFlightCounter counter;
  private final WorkerMetrics metrics;
  private final FailureResolver<K, V> failures;

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
   * @param metrics the metrics this component records
   */
  public SingleRecordWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      RecordHandler<K, V> handler,
      ProcessingLifecycleHook<K, V> hook,
      RetryExecutor<K, V> retryExecutor,
      ProcessingProgress offsetTracker,
      RecordDispatcher<K, V> dispatcher,
      InFlightCounter counter,
      WorkerMetrics metrics) {
    super(concurrency, threadMode, "kafka-pipeline-worker-", 0);
    this.handler = handler;
    this.hook = hook;
    this.retryExecutor = retryExecutor;
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.counter = counter;
    this.metrics = metrics;
    this.failures = new FailureResolver<>(retryExecutor, offsetTracker);
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
          rejected = reject(rejected, tp, record.offset());
          break;
        }
        try {
          if (!offsetTracker.registerUnlessDone(tp, record.offset())) {
            // Already processed before a crash or rebalance; recorded as done, not dispatched.
            continue;
          }
        } catch (WindowFullException e) {
          rejected = reject(rejected, tp, record.offset());
          break;
        } catch (RuntimeException e) {
          // E.g. the partition is assigned but not initialized. Send this partition's records back
          // and carry on with the others: the consumer position has already moved past every
          // polled record, so a partition skipped here would lose records once later ones commit.
          reportDispatchFailure(tp, record.offset(), e);
          rejected = reject(rejected, tp, record.offset());
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
        RecordDispatcher.PollResult<K, V> result = dispatcher.poll(100, MILLISECONDS);
        if (result != null) {
          try {
            processRecord(result.partition(), result.record(), result.generation());
          } finally {
            dispatcher.release(result);
          }
        }
      } catch (Throwable t) {
        // Never let a throwable end the loop: the pool would silently lose a worker.
        reportWorkerError(t);
      }
      clearStrayInterrupt();
    }
  }

  /**
   * Clears an interrupt left on this long-lived worker thread while the pool is running. Only a
   * shutdown (which stops the pool before interrupting) may end the loop; a handler that restores
   * the interrupt flag after catching InterruptedException must not.
   */
  private void clearStrayInterrupt() {
    if (running.get() && Thread.interrupted()) {
      logger.log(System.Logger.Level.DEBUG, "Cleared an interrupt left by the handler");
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

    if (skippedByHook(record, new ProcessingContext(tp, offset, 0))) {
      ack(tp, offset, generation);
      metrics.recordSkipped(1);
      return;
    }

    String desc = "offset " + offset;
    int[] lastAttempt = {0};
    Exception lastError =
        retryExecutor.executeWithRetries(
            attempt -> {
              lastAttempt[0] = attempt;
              handleOnce(record, new ProcessingContext(tp, offset, attempt));
            },
            tp,
            desc,
            this::stopping);

    if (lastError == null) {
      // Outside the retry loop: a hook that throws must not re-run a handler that succeeded.
      notifyAfterProcess(record, new ProcessingContext(tp, offset, lastAttempt[0]));
      ack(tp, offset, generation);
      metrics.recordProcessed(1);
      return;
    }

    failures.resolve(List.of(record), tp, lastError, desc, () -> ack(tp, offset, generation));
  }

  /** Asks the hook whether to skip the record; a hook that throws does not skip it. */
  private boolean skippedByHook(ConsumerRecord<K, V> record, ProcessingContext ctx) {
    try {
      return !hook.beforeProcess(record, ctx);
    } catch (Throwable e) {
      FatalErrors.rethrowIfJvmFatal(e);
      logger.log(
          System.Logger.Level.WARNING,
          "beforeProcess hook threw for {0}:{1}, proceeding with handler",
          ctx.partition(),
          ctx.offset());
      return false;
    }
  }

  private void handleOnce(ConsumerRecord<K, V> record, ProcessingContext ctx) throws Exception {
    try {
      handler.handle(record);
    } catch (Exception e) {
      notifyError(record, ctx, e);
      throw e;
    } catch (Error e) {
      if (!FatalErrors.isJvmFatal(e)) notifyError(record, ctx, RetryExecutor.asException(e));
      throw e;
    }
  }

  /** Calls onError; a hook that throws must not hide the handler's exception. */
  private void notifyError(ConsumerRecord<K, V> record, ProcessingContext ctx, Exception error) {
    try {
      hook.onError(record, ctx, error);
    } catch (Throwable hookError) {
      FatalErrors.rethrowIfJvmFatal(hookError);
      error.addSuppressed(hookError);
      logger.log(
          System.Logger.Level.WARNING,
          "onError hook threw for "
              + ctx.partition()
              + ":"
              + ctx.offset()
              + "; keeping the handler's exception",
          hookError);
    }
  }

  private void notifyAfterProcess(ConsumerRecord<K, V> record, ProcessingContext ctx) {
    try {
      hook.afterProcess(record, ctx);
    } catch (Throwable e) {
      FatalErrors.rethrowIfJvmFatal(e);
      logger.log(
          System.Logger.Level.WARNING,
          "afterProcess hook threw for "
              + ctx.partition()
              + ":"
              + ctx.offset()
              + "; the record is still acked",
          e);
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
