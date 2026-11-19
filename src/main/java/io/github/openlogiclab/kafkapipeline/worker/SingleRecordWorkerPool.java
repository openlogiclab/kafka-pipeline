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
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.ProcessorBinding;
import io.github.openlogiclab.kafkapipeline.internal.RecordSize;
import io.github.openlogiclab.kafkapipeline.internal.WorkerMetrics;
import io.github.openlogiclab.kafkapipeline.internal.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.offset.ProcessingProgress;
import io.github.openlogiclab.kafkapipeline.offset.WindowFullException;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Per-record processing mode. Worker threads take records from the {@link RecordDispatcher}'s lanes
 * and run:
 *
 * <pre>
 *   markInProgress → hook.beforeProcess
 *       → processor (with retries; a plain handler is a processor with no outputs)
 *       → no outputs:   hook.afterProcess → ack
 *       → outputs:      sink.write, the worker moves on   ({@link SinkWrites})
 *             → durable: hook.afterProcess → ack
 *             → failed:  retry the write → DLQ → FinalFailureHandler → drop
 *       → failure: DLQ → FinalFailureHandler → drop
 * </pre>
 *
 * <p>The record holds its in-flight slot (global backpressure) and its dispatcher slot (ordering,
 * per-partition limit) until it is acked or dropped, including while its write is in flight.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class SingleRecordWorkerPool<K, V> extends WorkerPool<K, V> {

  private static final System.Logger logger =
      System.getLogger(SingleRecordWorkerPool.class.getName());

  /**
   * How long an idle worker waits for a record before checking whether the pool is stopping.
   * Dispatching wakes a waiting worker at once, so this only bounds the cost of a missed wakeup and
   * how quickly an idle worker notices shutdown.
   */
  private static final long IDLE_WAIT_MILLIS = 100;

  private final ProcessorBinding<K, V, ?> binding;
  private final SinkWrites writes;
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
    this(
        concurrency,
        threadMode,
        ProcessorBinding.forHandler(handler),
        hook,
        retryExecutor,
        offsetTracker,
        dispatcher,
        counter,
        metrics);
  }

  /**
   * Creates a worker pool that runs a processor and writes its outputs to a sink. A record is acked
   * only once its outputs are durable; until then it stays in flight for backpressure, in progress
   * in its partition window, and holds its key or partition slot in ordered modes.
   *
   * @param concurrency number of worker threads
   * @param threadMode platform or virtual threads
   * @param binding the processor and its sink (or a handler bound as a processor without outputs)
   * @param hook the lifecycle hook
   * @param retryExecutor the retry executor; its error strategy also governs write retries
   * @param offsetTracker the offset tracker
   * @param dispatcher the record dispatcher
   * @param counter the in-flight counter
   * @param metrics the worker metrics
   */
  public SingleRecordWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      ProcessorBinding<K, V, ?> binding,
      ProcessingLifecycleHook<K, V> hook,
      RetryExecutor<K, V> retryExecutor,
      ProcessingProgress offsetTracker,
      RecordDispatcher<K, V> dispatcher,
      InFlightCounter counter,
      WorkerMetrics metrics) {
    super(concurrency, threadMode, "kafka-pipeline-worker-", 0);
    this.binding = binding;
    this.hook = hook;
    this.retryExecutor = retryExecutor;
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.counter = counter;
    this.metrics = metrics;
    this.failures =
        new FailureResolver<>(retryExecutor, offsetTracker, this::stopping, this::escalateFatal);
    this.writes = new SinkWrites(retryExecutor, this::stopping, this::escalateFatal);
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

  /**
   * Stops the workers, then waits for writes still in flight, within the same timeout: sinks are
   * flushed for every assigned partition, and records whose writes complete in time are acked, so
   * the final commit includes them. Writes still pending after that are abandoned; their records
   * are not acked and are redelivered.
   *
   * @param timeoutMs max time to wait in milliseconds
   */
  @Override
  public void stop(long timeoutMs) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    super.stop(timeoutMs);
    try {
      if (binding.sink() != null) {
        flushSink(deadline);
        if (!writes.awaitIdle(deadline)) {
          logger.log(
              System.Logger.Level.WARNING,
              "{0} sink writes still pending at shutdown; their records will be redelivered",
              writes.pending());
        }
      }
    } finally {
      writes.close();
    }
  }

  private void flushSink(long deadline) {
    List<CompletableFuture<Void>> flushes = new ArrayList<>();
    for (TopicPartition tp : dispatcher.partitions()) {
      try {
        flushes.add(binding.sink().flush(tp).toCompletableFuture());
      } catch (Throwable t) {
        FatalErrors.rethrowIfJvmFatal(t);
        logger.log(System.Logger.Level.WARNING, "Sink flush failed for " + tp, t);
      }
    }
    if (flushes.isEmpty()) return;
    try {
      CompletableFuture.allOf(flushes.toArray(CompletableFuture[]::new))
          .get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
      logger.log(System.Logger.Level.WARNING, "Sink flush did not finish at shutdown", e);
    }
  }

  @Override
  protected void onStopping() {
    retryExecutor.beginShutdown(); // ends processor-retry, write-retry and DLQ backoffs at once
  }

  @Override
  public void onPartitionsDraining(Collection<TopicPartition> partitions) {
    writes.draining(partitions);
  }

  @Override
  public void onPartitionsDrained(Collection<TopicPartition> partitions) {
    writes.drained(partitions);
    retryExecutor.partitionsChanged(); // retries of their records give up at once
  }

  /** Number of records whose sink write is still in flight (for tests). */
  int pendingWrites() {
    return writes.pending();
  }

  @Override
  public boolean canResume(TopicPartition tp) {
    return dispatcher.hasDrained(tp);
  }

  private void workerLoop() {
    while (running.get()) {
      try {
        RecordDispatcher.PollResult<K, V> result = dispatcher.poll(IDLE_WAIT_MILLIS, MILLISECONDS);
        if (result != null) {
          Completion done = new Completion(result);
          boolean deferred = false;
          try {
            deferred = process(result, done);
          } finally {
            // A record whose write is in flight is finished by the write's outcome instead.
            if (!deferred) done.finish();
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

  /**
   * Releases what a polled record holds, exactly once: its in-flight slot (backpressure) and its
   * dispatcher slot (per-partition limit, key or partition order). Always runs, even if the
   * partition was revoked meanwhile and the ack failed; otherwise backpressure would never drain.
   */
  private final class Completion {
    private final RecordDispatcher.PollResult<K, V> result;
    private final AtomicBoolean finished = new AtomicBoolean();

    Completion(RecordDispatcher.PollResult<K, V> result) {
      this.result = result;
    }

    void finish() {
      if (!finished.compareAndSet(false, true)) return;
      try {
        counter.completed(1, RecordSize.estimateBytes(result.record()));
      } finally {
        dispatcher.release(result);
      }
    }
  }

  /**
   * Processes a record and starts writing its outputs.
   *
   * @return true if a write is in flight and its outcome will finish the record
   */
  private boolean process(RecordDispatcher.PollResult<K, V> result, Completion done) {
    TopicPartition tp = result.partition();
    ConsumerRecord<K, V> record = result.record();
    long generation = result.generation();
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
      return false;
    }

    if (skippedByHook(record, new ProcessingContext(tp, offset, 0))) {
      ack(tp, offset, generation);
      metrics.recordSkipped(1);
      return false;
    }
    return run(binding, tp, record, generation, done);
  }

  private <O> boolean run(
      ProcessorBinding<K, V, O> binding,
      TopicPartition tp,
      ConsumerRecord<K, V> record,
      long generation,
      Completion done) {
    long offset = record.offset();
    String desc = "offset " + offset;
    int[] lastAttempt = {0};
    List<O> outputs = new ArrayList<>(1);
    Exception lastError =
        retryExecutor.executeWithRetries(
            attempt -> {
              lastAttempt[0] = attempt;
              outputs.clear();
              outputs.addAll(
                  processOnce(binding, record, new ProcessingContext(tp, offset, attempt)));
            },
            tp,
            desc,
            this::stopping,
            () -> owns(offsetTracker, tp, generation));

    if (lastError != null) {
      failures.resolve(
          List.of(record),
          tp,
          lastError,
          desc,
          () -> ack(tp, offset, generation),
          () -> owns(offsetTracker, tp, generation));
      return false;
    }

    ProcessingContext ctx = new ProcessingContext(tp, offset, lastAttempt[0]);
    if (outputs.isEmpty()) {
      succeeded(record, ctx, generation);
      return false;
    }

    writes.write(
        binding,
        new SinkContext(tp, offset, record.headers()),
        List.copyOf(outputs),
        () -> owns(offsetTracker, tp, generation),
        new SinkWrites.Outcome() {
          @Override
          public void succeeded() {
            try {
              SingleRecordWorkerPool.this.succeeded(record, ctx, generation);
            } finally {
              done.finish();
            }
          }

          @Override
          public void failed(Exception error) {
            try {
              failures.resolve(
                  List.of(record),
                  tp,
                  error,
                  "sink write of " + desc,
                  () -> ack(tp, offset, generation),
                  () -> owns(offsetTracker, tp, generation));
            } finally {
              done.finish();
            }
          }

          @Override
          public void abandoned(Exception reason) {
            try {
              logger.log(
                  System.Logger.Level.DEBUG,
                  "Sink write for {0} {1} abandoned ({2}), not acking; it will be redelivered",
                  tp,
                  desc,
                  reason.toString());
            } finally {
              done.finish();
            }
          }
        });
    return true;
  }

  /** The record is done: its processing succeeded and its outputs, if any, are durable. */
  private void succeeded(ConsumerRecord<K, V> record, ProcessingContext ctx, long generation) {
    // Outside the retry loop: a hook that throws must not re-run a processor that succeeded.
    notifyAfterProcess(record, ctx);
    ack(ctx.partition(), ctx.offset(), generation);
    metrics.recordProcessed(1);
  }

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

  private <O> List<O> processOnce(
      ProcessorBinding<K, V, O> binding, ConsumerRecord<K, V> record, ProcessingContext ctx)
      throws Exception {
    try {
      return binding.process(record);
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
