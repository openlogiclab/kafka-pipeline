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

import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import io.github.openlogiclab.kafkapipeline.internal.Causes;
import io.github.openlogiclab.kafkapipeline.internal.Classification;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.ProcessorBinding;
import io.github.openlogiclab.kafkapipeline.internal.RetryMetrics;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Runs sink writes for records whose processing succeeded, retries failed writes, and reports the
 * outcome exactly once per record.
 *
 * <p>The worker that processed a record hands its output here and moves on. When the write stage
 * completes, the outcome runs on a virtual thread of this class, never on the sink's own I/O thread
 * (for Kafka, the producer's sender thread), so outcomes may ack, call hooks or send to the DLQ
 * without stalling the sink.
 *
 * <p>A failed write is classified by the sink ({@code RecordSink.classify}) by what it says about
 * the destination:
 *
 * <ul>
 *   <li><b>{@code FATAL}</b> (e.g. a missing ACL on the output topic; always for JVM errors): every
 *       record would fail the same way, so the pipeline stops and the record is left unacked.
 *       Dropping it would commit input whose output was never written.
 *   <li><b>{@code TRANSIENT}</b> (e.g. a broker timeout): the destination is unavailable for now.
 *       The write is retried, with backoff, for as long as the pipeline runs and owns the
 *       partition, without using up the record's retry budget. The record holds its slot meanwhile,
 *       so an outage turns into backpressure, not into dropped records. If the outage outlasts
 *       {@code writeOutageTimeout}, the pipeline stops (still without dropping the record), so a
 *       destination that never comes back is noticed.
 *   <li><b>{@code RECORD}</b> (e.g. a record too large): retried up to the error strategy's {@code
 *       maxRetries}, then the record goes to the DLQ or is dropped.
 * </ul>
 *
 * <p>Retrying only re-runs the write, never the processor. A write is abandoned, leaving its record
 * unacked for redelivery, when the pipeline stops or the partition is revoked in the meantime, so
 * an old assignment never retries or dead-letters a record that now belongs to another owner.
 *
 * <p>While a partition is being revoked, every write of it is followed by a {@code flush}, so a
 * sink that batches (such as the Kafka producer with a long {@code linger.ms}) does not hold back
 * the records the drain is waiting for.
 */
final class SinkWrites {

  private static final System.Logger logger = System.getLogger(SinkWrites.class.getName());

  /** What happens to a record once its write has finished, one way or the other. */
  interface Outcome {
    /** The output is durable: ack the record. */
    void succeeded();

    /**
     * The write failed for this record and its retries are used up: send it to the DLQ or drop it.
     *
     * @param error the last error
     */
    void failed(Exception error);

    /**
     * Gave up without deciding: the pipeline is stopping, the partition moved to another
     * assignment, or a fatal error stops the pipeline. Leave the record unacked so it is
     * redelivered.
     *
     * @param reason why
     */
    void abandoned(Exception reason);
  }

  private final RetryExecutor<?, ?> retries;
  private final ErrorStrategy<?, ?> strategy;
  private final RetryMetrics metrics;
  private final BooleanSupplier stopping;
  private final Consumer<Throwable> onFatal;
  // Partitions being revoked: their writes are flushed at once (see the class comment).
  private final Set<TopicPartition> draining = ConcurrentHashMap.newKeySet();
  private final ExecutorService outcomes =
      Executors.newThreadPerTaskExecutor(
          Thread.ofVirtual().name("kafka-pipeline-sink-", 0).factory());

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition idle = lock.newCondition();
  private int pending; // guarded by lock

  /**
   * Creates the write runner.
   *
   * @param retries the retry policy, backoff, shutdown signal and outage timeout
   * @param stopping whether the pipeline is stopping
   * @param onFatal stops the pipeline; called with a fatal write error or a JVM error
   */
  SinkWrites(RetryExecutor<?, ?> retries, BooleanSupplier stopping, Consumer<Throwable> onFatal) {
    this.retries = retries;
    this.strategy = retries.strategy();
    this.metrics = retries.metrics();
    this.stopping = stopping;
    this.onFatal = onFatal;
  }

  /**
   * The partitions are being revoked: from now on, flush the sink after each of their writes, so
   * the drain does not wait for a batching sink to send on its own schedule.
   */
  void draining(Collection<TopicPartition> partitions) {
    draining.addAll(partitions);
  }

  /** The partitions are gone (drained, or lost): back to normal. */
  void drained(Collection<TopicPartition> partitions) {
    draining.removeAll(partitions);
  }

  /** One record's write and everything needed to retry or abandon it. */
  private static final class Write<O> {
    final ProcessorBinding<?, ?, O> binding;
    final SinkContext ctx;
    final O output;
    final BooleanSupplier owned;
    final Outcome outcome;
    // Set by the first report: the outcome runs, and the pending count drops, exactly once.
    final AtomicBoolean reported = new AtomicBoolean();
    long outageStartNanos; // when this write started failing transiently; one thread at a time
    int transientAttempts; // consecutive transient failures

    Write(
        ProcessorBinding<?, ?, O> binding,
        SinkContext ctx,
        O output,
        BooleanSupplier owned,
        Outcome outcome) {
      this.binding = binding;
      this.ctx = ctx;
      this.output = output;
      this.owned = owned;
      this.outcome = outcome;
    }
  }

  /**
   * Starts writing a record's output. The outcome is reported exactly once, unless this method
   * throws, in which case it is never reported.
   *
   * @param owned whether this consumer still owns the record's partition in the assignment the
   *     record was fetched under
   */
  <O> void write(
      ProcessorBinding<?, ?, O> binding,
      SinkContext ctx,
      O output,
      BooleanSupplier owned,
      Outcome outcome) {
    lock.lock();
    try {
      pending++;
    } finally {
      lock.unlock();
    }
    try {
      attempt(new Write<>(binding, ctx, output, owned, outcome), 0);
    } catch (Throwable t) {
      finished(); // the outcome will never run
      throw t;
    }
  }

  private <O> void attempt(Write<O> write, int attempt) {
    CompletionStage<Void> stage = write.binding.write(write.ctx, write.output);
    if (draining.contains(write.ctx.partition())) flush(write);
    stage.whenComplete(
        (ignored, error) -> {
          // Runs on the sink's I/O thread: only hand off. CompletableFuture would swallow anything
          // thrown here, which would leak the record's slots, so nothing may escape.
          try {
            outcomes.execute(() -> handleResult(write, attempt, error));
          } catch (Throwable t) {
            // Closed (the pipeline gave up waiting for this write) or out of memory.
            if (FatalErrors.isJvmFatal(t)) onFatal.accept(t);
            report(write, () -> write.outcome.abandoned(RetryExecutor.asException(t)));
          }
        });
  }

  /**
   * Handles a completed write, so that its outcome is reported whatever goes wrong here: an
   * unexpected error fails the record through the error strategy (DLQ or drop) instead of leaving
   * it in flight forever.
   */
  private <O> void handleResult(Write<O> write, int attempt, Throwable error) {
    try {
      onResult(write, attempt, error);
    } catch (Throwable t) {
      if (FatalErrors.isJvmFatal(t)) {
        onFatal.accept(t);
        report(write, () -> write.outcome.abandoned(RetryExecutor.asException(t)));
        return;
      }
      logger.log(
          System.Logger.Level.ERROR,
          "Unexpected error handling the sink write for " + write.ctx.recordId(),
          t);
      report(write, () -> write.outcome.failed(RetryExecutor.asException(t)));
    }
  }

  private <O> void onResult(Write<O> write, int attempt, Throwable error) {
    if (error == null) {
      report(write, write.outcome::succeeded);
      return;
    }
    Exception cause = RetryExecutor.asException(unwrap(error));
    WriteFailure kind = classify(write, unwrap(error), cause);
    if (kind == WriteFailure.FATAL) {
      logger.log(
          System.Logger.Level.ERROR,
          "Sink write for " + write.ctx.recordId() + " failed fatally, stopping the pipeline",
          cause);
      onFatal.accept(cause);
      report(write, () -> write.outcome.abandoned(cause));
      return;
    }
    if (giveUpForNow(write, cause)) return;

    Duration backoff;
    int next;
    if (kind == WriteFailure.TRANSIENT) {
      long now = System.nanoTime();
      if (write.transientAttempts++ == 0) write.outageStartNanos = now;
      if (now - write.outageStartNanos > retries.writeOutageTimeout().toNanos()) {
        Exception outage =
            new IllegalStateException(
                "Sink unavailable for more than " + retries.writeOutageTimeout(), cause);
        onFatal.accept(outage);
        report(write, () -> write.outcome.abandoned(outage));
        return;
      }
      retries.logOutage("Sink", cause);
      backoff = retries.transientBackoff(write.transientAttempts);
      next = attempt; // a transient failure does not use up the record's retry budget
    } else {
      write.transientAttempts = 0;
      if (attempt >= strategy.maxRetries()) {
        report(write, () -> write.outcome.failed(cause));
        return;
      }
      logger.log(
          System.Logger.Level.DEBUG,
          "Retry {0}/{1} of the sink write for {2}: {3}",
          attempt + 1,
          strategy.maxRetries(),
          write.ctx.recordId(),
          cause.toString());
      backoff = strategy.backoffForAttempt(attempt + 1);
      next = attempt + 1;
    }
    metrics.recordRetry();
    // Ends early on shutdown or when the partition leaves this consumer.
    retries.waitOutBackoff(backoff, () -> !stopping.getAsBoolean() && write.owned.getAsBoolean());
    Thread.interrupted(); // close() interrupts these threads; giveUpForNow() sees the shutdown
    if (giveUpForNow(write, cause)) return;
    try {
      attempt(write, next);
    } catch (Throwable t) {
      if (FatalErrors.isJvmFatal(t)) onFatal.accept(t);
      report(write, () -> write.outcome.abandoned(RetryExecutor.asException(t)));
    }
  }

  /** Abandons the write if the pipeline is stopping or the partition moved on; true if it did. */
  private boolean giveUpForNow(Write<?> write, Exception cause) {
    if (stopping.getAsBoolean() || !write.owned.getAsBoolean()) {
      report(write, () -> write.outcome.abandoned(cause));
      return true;
    }
    return false;
  }

  private static WriteFailure classify(Write<?> write, Throwable raw, Exception cause) {
    if (FatalErrors.isJvmFatal(raw)) return WriteFailure.FATAL;
    return Classification.classify(write.binding.sink()::classify, cause);
  }

  private void flush(Write<?> write) {
    try {
      write.binding.sink().flush(write.ctx.partition());
    } catch (Throwable t) {
      FatalErrors.rethrowIfJvmFatal(t);
      logger.log(System.Logger.Level.WARNING, "Sink flush failed for " + write.ctx.partition(), t);
    }
  }

  /** Runs an outcome, then counts the write as finished. */
  private void report(Write<?> write, Runnable outcome) {
    if (!write.reported.compareAndSet(false, true)) return;
    try {
      outcome.run();
    } catch (Throwable t) {
      if (FatalErrors.isJvmFatal(t)) {
        // Same as on a worker: the JVM may be unusable, so stop the pipeline. The record was not
        // acked and is redelivered.
        onFatal.accept(t);
      } else {
        logger.log(System.Logger.Level.ERROR, "Error completing a sink write", t);
      }
    } finally {
      finished();
    }
  }

  private void finished() {
    lock.lock();
    try {
      if (--pending == 0) idle.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** Number of writes whose outcome has not been reported yet. */
  int pending() {
    lock.lock();
    try {
      return pending;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Waits until every write has reported its outcome.
   *
   * @return true if none is pending any more
   */
  boolean awaitIdle(long deadlineNanos) {
    lock.lock();
    try {
      while (pending > 0) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) return false;
        idle.await(remaining, TimeUnit.NANOSECONDS);
      }
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return pending == 0;
    } finally {
      lock.unlock();
    }
  }

  /** Stops running outcomes. Writes still pending are reported abandoned if they finish. */
  void close() {
    outcomes.shutdownNow();
  }

  private static Throwable unwrap(Throwable t) {
    // Bounded: wrappers may form a cycle.
    for (int i = 0;
        i < Causes.MAX_DEPTH
            && (t instanceof CompletionException || t instanceof ExecutionException)
            && t.getCause() != null;
        i++) {
      t = t.getCause();
    }
    return t;
  }
}
