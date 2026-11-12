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

import io.github.openlogiclab.kafkapipeline.error.DLQHandler;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.HandlerAttemptObserver;
import io.github.openlogiclab.kafkapipeline.internal.RetryMetrics;
import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Unified retry → DLQ → skip engine shared by both per-record and batch processing paths.
 *
 * <p>Best-effort strategy: retries first, then DLQ, then final failure handler + skip. The system
 * never halts — failed records are always skipped after exhausting all recovery options.
 *
 * <p>Stateless — all state lives in the caller or in {@link ErrorStrategy}. Thread-safe as long as
 * the {@link ErrorStrategy} and its {@link DLQHandler} are thread-safe.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class RetryExecutor<K, V> {

  private static final System.Logger logger = System.getLogger(RetryExecutor.class.getName());

  private final ErrorStrategy<K, V> strategy;
  private final RetryMetrics metrics;
  private final HandlerAttemptObserver attempts;
  private final AttemptTimer timer;

  /**
   * Creates a retry executor with no processing timeout that reports attempts to no one.
   *
   * @param strategy the error strategy
   * @param metrics the metrics this component records
   */
  public RetryExecutor(ErrorStrategy<K, V> strategy, RetryMetrics metrics) {
    this(strategy, metrics, HandlerAttemptObserver.NONE, Duration.ZERO);
  }

  /**
   * Creates a retry executor that interrupts an attempt running longer than {@code
   * processingTimeout} and counts it as failed.
   *
   * @param strategy the error strategy
   * @param metrics the metrics this component records
   * @param attempts observes every handler attempt
   * @param processingTimeout max duration of one handler attempt; zero or null disables it
   */
  public RetryExecutor(
      ErrorStrategy<K, V> strategy,
      RetryMetrics metrics,
      HandlerAttemptObserver attempts,
      Duration processingTimeout) {
    this.strategy = strategy;
    this.metrics = metrics;
    this.attempts = Objects.requireNonNull(attempts, "attempts");
    this.timer =
        processingTimeout == null || processingTimeout.isZero() || processingTimeout.isNegative()
            ? AttemptTimer.DISABLED
            : new AttemptTimer(processingTimeout);
  }

  /** Stops the processing-timeout timer thread, if any. */
  public void close() {
    timer.close();
  }

  /** A task that can be retried. */
  @FunctionalInterface
  public interface RetryableTask {
    /**
     * Executes the task.
     *
     * @param attempt the attempt number (0-based)
     * @throws Exception if processing fails
     */
    void execute(int attempt) throws Exception;
  }

  /** Resolution of a failure after retries. */
  public enum FailureResolution {
    /** All records sent to DLQ successfully. */
    DLQ_SUCCESS,
    /**
     * At least one record was dropped (DLQ not configured or the DLQ send failed). Records sent to
     * the DLQ before a failure still count as DLQ'd in the metrics.
     */
    SKIPPED
  }

  /**
   * Executes a task with retries according to the error strategy. Equivalent to {@link
   * #executeWithRetries(RetryableTask, TopicPartition, String, BooleanSupplier)} where every
   * interrupt is treated as a shutdown.
   *
   * @param task the task to execute
   * @param tp the topic partition for logging
   * @param description description for logging
   * @return null if successful, otherwise the last exception
   */
  public Exception executeWithRetries(RetryableTask task, TopicPartition tp, String description) {
    return executeWithRetries(task, tp, description, () -> true);
  }

  /**
   * Executes a task with retries according to the error strategy.
   *
   * <p>Anything the task throws counts as a failed attempt, including {@link Error}s, except a
   * JVM-fatal error such as {@link OutOfMemoryError} (see {@link FatalErrors#isJvmFatal}), which is
   * rethrown: the JVM may be unusable and the record must not be skipped. An interrupt counts as a
   * shutdown only while {@code stopping} is true; in that case the method returns at once with the
   * thread's interrupt flag set. An interrupt the handler leaves behind while the pipeline is
   * running is cleared and counted as an ordinary failure. An attempt that runs past the processing
   * timeout is interrupted and fails with a {@link TimeoutException}.
   *
   * @param task the task to execute
   * @param tp the topic partition for logging
   * @param description description for logging
   * @param stopping whether the pipeline is shutting down
   * @return null if successful, otherwise the last exception
   */
  public Exception executeWithRetries(
      RetryableTask task, TopicPartition tp, String description, BooleanSupplier stopping) {
    Exception lastError = null;
    int maxAttempts = 1 + strategy.maxRetries();

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        try {
          Thread.sleep(strategy.backoffForAttempt(attempt).toMillis());
        } catch (InterruptedException e) {
          if (stopping.getAsBoolean()) {
            Thread.currentThread().interrupt();
            return e;
          }
          // Not a shutdown: an interrupt left over on this thread. Retry now.
        }
      }

      AttemptTimer.Attempt timed = timer.start();
      long startNanos = System.nanoTime();
      Throwable failure = null;
      try {
        task.execute(attempt);
      } catch (Throwable t) {
        failure = t;
      }
      long elapsedNanos = System.nanoTime() - startNanos;
      boolean timedOut = timed.finish();
      attempts.onAttempt(tp, elapsedNanos, outcomeOf(failure, timedOut));

      if (failure == null) {
        if (timedOut) {
          logger.log(
              System.Logger.Level.WARNING,
              "{0} {1} finished after the processing timeout ({2}); treating it as processed",
              tp,
              description,
              timer.timeout());
        }
        if (!stopping.getAsBoolean()) Thread.interrupted(); // drop an interrupt the handler leaked
        return null;
      }
      FatalErrors.rethrowIfJvmFatal(failure);

      if (timedOut && stopping.getAsBoolean()) {
        // finish() cleared the timer's interrupt, which may have hidden a shutdown interrupt
        // delivered meanwhile. Shutting down: leave the record unacked for redelivery.
        Thread.currentThread().interrupt();
        return shutdownInterrupt(failure);
      } else if (timedOut) {
        TimeoutException timeout =
            new TimeoutException(
                description + " of " + tp + " exceeded the processing timeout " + timer.timeout());
        timeout.initCause(failure);
        lastError = timeout;
        metrics.recordProcessingTimeout();
      } else if (Thread.currentThread().isInterrupted() || causedByInterrupt(failure)) {
        if (stopping.getAsBoolean()) {
          Thread.currentThread().interrupt();
          return shutdownInterrupt(failure);
        }
        // The handler was interrupted (or left the flag set) while the pipeline is running. That
        // is a processing failure, not a shutdown; clear it so it cannot end the worker.
        Thread.interrupted();
        lastError = asException(failure);
      } else {
        lastError = asException(failure);
      }

      if (attempt < maxAttempts - 1) {
        metrics.recordRetry();
        logger.log(
            System.Logger.Level.DEBUG,
            "Retry {0}/{1} for {2}: {3}",
            attempt + 1,
            strategy.maxRetries(),
            tp,
            description);
      }
    }
    return lastError;
  }

  private static AttemptOutcome outcomeOf(Throwable failure, boolean timedOut) {
    if (failure == null) return AttemptOutcome.SUCCESS; // includes finishing after the timeout
    return timedOut ? AttemptOutcome.TIMEOUT : AttemptOutcome.FAILURE;
  }

  /**
   * Returns the throwable as an Exception, wrapping Errors so handlers and the DLQ can see them.
   */
  static Exception asException(Throwable t) {
    return t instanceof Exception e ? e : new RuntimeException("Handler threw " + t, t);
  }

  private static InterruptedException shutdownInterrupt(Throwable failure) {
    if (failure instanceof InterruptedException ie) return ie;
    InterruptedException ie = new InterruptedException("Interrupted by shutdown");
    ie.initCause(failure);
    return ie;
  }

  private static boolean causedByInterrupt(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof InterruptedException) return true;
      if (c.getCause() == c) break;
    }
    return false;
  }

  /**
   * Handles a failure after retries are exhausted. Based on the fallback strategy:
   *
   * <ul>
   *   <li>{@code SKIP}: skip directly, no DLQ attempt
   *   <li>{@code DLQ_THEN_SKIP}: try DLQ if configured, skip if DLQ fails
   * </ul>
   *
   * @param records the failed records
   * @param tp the topic partition
   * @param error the exception that caused failure
   * @param description description for logging
   * @return DLQ_SUCCESS if all records were sent to DLQ, SKIPPED otherwise
   */
  public FailureResolution handleFailure(
      List<ConsumerRecord<K, V>> records, TopicPartition tp, Exception error, String description) {

    int recordCount = records.size();

    // DLQ_THEN_SKIP: try DLQ if configured; SKIP: skip directly
    if (strategy.fallback() == Fallback.DLQ_THEN_SKIP && strategy.hasDlq()) {
      int sentToDlq = 0;
      try {
        DLQHandler<K, V> dlq = strategy.dlqHandler();
        for (ConsumerRecord<K, V> record : records) {
          dlq.send(record, error);
          sentToDlq++;
        }
        metrics.recordDlqSuccess(recordCount);
        logger.log(System.Logger.Level.INFO, "Sent to DLQ: {0} ({1})", tp, description);
        return FailureResolution.DLQ_SUCCESS;
      } catch (Throwable dlqThrowable) {
        FatalErrors.rethrowIfJvmFatal(dlqThrowable);
        Exception dlqError = asException(dlqThrowable);
        int failedToSend = recordCount - sentToDlq;
        if (sentToDlq > 0) {
          metrics.recordDlqSuccess(sentToDlq);
          logger.log(
              System.Logger.Level.WARNING,
              "Partial DLQ send for {0} ({1}): {2}/{3} records sent before failure",
              tp,
              description,
              sentToDlq,
              recordCount);
        }
        metrics.recordDlqFailure(failedToSend);
        logger.log(
            System.Logger.Level.ERROR,
            "DLQ send failed for {0} ({1}): {2}",
            tp,
            description,
            dlqError.getMessage());

        // Call final failure handler for records that couldn't be sent to DLQ
        for (int i = sentToDlq; i < recordCount; i++) {
          invokeFinalFailureHandler(records.get(i), dlqError);
        }
        // Only the records that did not reach the DLQ are dropped; the others count as DLQ'd.
        metrics.recordFailed(failedToSend);
      }
    } else {
      // No DLQ configured, call final failure handler for all records
      for (ConsumerRecord<K, V> record : records) {
        invokeFinalFailureHandler(record, error);
      }
      metrics.recordFailed(recordCount);
    }

    return FailureResolution.SKIPPED;
  }

  private void invokeFinalFailureHandler(ConsumerRecord<K, V> record, Exception error) {
    metrics.recordFinalFailure();
    try {
      strategy.finalFailureHandler().handle(record, error);
    } catch (Throwable handlerError) {
      FatalErrors.rethrowIfJvmFatal(handlerError);
      logger.log(
          System.Logger.Level.WARNING,
          "FinalFailureHandler threw exception for {0}:{1}:{2}: {3}",
          record.topic(),
          record.partition(),
          record.offset(),
          handlerError.getMessage());
    }
  }
}
