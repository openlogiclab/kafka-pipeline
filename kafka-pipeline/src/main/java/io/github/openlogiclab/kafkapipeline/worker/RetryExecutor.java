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
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import io.github.openlogiclab.kafkapipeline.internal.Causes;
import io.github.openlogiclab.kafkapipeline.internal.Classification;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.HandlerAttemptObserver;
import io.github.openlogiclab.kafkapipeline.internal.LogRateLimit;
import io.github.openlogiclab.kafkapipeline.internal.RetryMetrics;
import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Unified retry → DLQ → drop engine shared by both per-record and batch processing paths.
 *
 * <p>Retries a failed processing attempt; when the retries are used up, sends the record to the DLQ
 * or drops it. A bad record never stops the pipeline. An unavailable DLQ does not get records
 * dropped either: transient DLQ failures are retried, fatal ones stop the pipeline.
 *
 * <p>Holds no per-record state; the only shared state is the shutdown signal that ends retry
 * backoffs. Thread-safe as long as the {@link ErrorStrategy} and its {@link DLQHandler} are
 * thread-safe.
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
  private final Duration writeOutageTimeout;
  // Retry backoffs wait on this; it is signalled when the pipeline starts stopping or partitions
  // leave this consumer, so a retry that should give up never sits out the rest of its backoff.
  private final ReentrantLock backoffLock = new ReentrantLock();
  private final Condition backoffWake = backoffLock.newCondition();
  private volatile boolean shuttingDown;
  private final LogRateLimit outageLog = new LogRateLimit();

  /** Default {@code writeOutageTimeout}: how long a destination may stay unavailable. */
  public static final Duration DEFAULT_WRITE_OUTAGE_TIMEOUT = Duration.ofMinutes(5);

  /** Shortest wait between retries of a transient write failure, so a zero backoff cannot spin. */
  static final Duration MIN_TRANSIENT_BACKOFF = Duration.ofMillis(100);

  /** Longest wait between retries of a transient write failure, so recovery is noticed quickly. */
  static final Duration MAX_TRANSIENT_BACKOFF = Duration.ofSeconds(30);

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
    this(strategy, metrics, attempts, processingTimeout, DEFAULT_WRITE_OUTAGE_TIMEOUT);
  }

  /**
   * Creates a retry executor.
   *
   * @param strategy the error strategy
   * @param metrics the metrics this component records
   * @param attempts observes every handler attempt
   * @param processingTimeout max duration of one handler attempt; zero or null disables it
   * @param writeOutageTimeout how long a sink or DLQ write may keep failing transiently before the
   *     pipeline stops
   */
  public RetryExecutor(
      ErrorStrategy<K, V> strategy,
      RetryMetrics metrics,
      HandlerAttemptObserver attempts,
      Duration processingTimeout,
      Duration writeOutageTimeout) {
    this.writeOutageTimeout = Objects.requireNonNull(writeOutageTimeout, "writeOutageTimeout");
    this.strategy = strategy;
    this.metrics = metrics;
    this.attempts = Objects.requireNonNull(attempts, "attempts");
    this.timer =
        processingTimeout == null || processingTimeout.isZero() || processingTimeout.isNegative()
            ? AttemptTimer.DISABLED
            : new AttemptTimer(processingTimeout);
  }

  /** The error strategy, so asynchronous sink writes retry with the same policy. */
  ErrorStrategy<K, V> strategy() {
    return strategy;
  }

  /** The retry metrics, shared with asynchronous sink write retries. */
  RetryMetrics metrics() {
    return metrics;
  }

  /** How long a destination may stay unavailable before the pipeline stops. */
  Duration writeOutageTimeout() {
    return writeOutageTimeout;
  }

  /**
   * The pipeline is stopping: end every retry backoff now, so retrying records give up at once
   * (leaving them unacked for redelivery) instead of holding shutdown for the rest of a backoff.
   */
  public void beginShutdown() {
    shuttingDown = true;
    wakeBackoffs();
  }

  /**
   * Partitions left this consumer: wake every retry backoff, so retries of their records notice at
   * once that they no longer own them and give up.
   */
  public void partitionsChanged() {
    wakeBackoffs();
  }

  private void wakeBackoffs() {
    backoffLock.lock();
    try {
      backoffWake.signalAll();
    } finally {
      backoffLock.unlock();
    }
  }

  /**
   * Waits out a backoff. Returns early when the pipeline starts stopping, when {@code keepWaiting}
   * turns false after a wake-up ({@link #partitionsChanged()}), or when the thread is interrupted
   * (the interrupt flag is kept; the caller decides whether it means shutdown).
   *
   * @return true if the wait ended because of shutdown or an interrupt
   */
  boolean waitOutBackoff(Duration backoff, BooleanSupplier keepWaiting) {
    long deadline = System.nanoTime() + backoff.toNanos();
    backoffLock.lock();
    try {
      while (!shuttingDown && keepWaiting.getAsBoolean()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) return false;
        backoffWake.awaitNanos(remaining);
      }
      return shuttingDown;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return true;
    } finally {
      backoffLock.unlock();
    }
  }

  /** Backoff before retry {@code attempt} of a transient write failure. */
  Duration transientBackoff(int attempt) {
    Duration backoff = strategy.backoffForAttempt(attempt);
    if (backoff.compareTo(MIN_TRANSIENT_BACKOFF) < 0) return MIN_TRANSIENT_BACKOFF;
    if (backoff.compareTo(MAX_TRANSIENT_BACKOFF) > 0) return MAX_TRANSIENT_BACKOFF;
    return backoff;
  }

  /** Logs, at most once per interval, that a destination is unavailable and being retried. */
  void logOutage(String destination, Throwable cause) {
    if (outageLog.tryAcquire()) {
      logger.log(
          System.Logger.Level.WARNING,
          "{0} unavailable ({1}); retrying until it recovers, at most {2}",
          destination,
          cause.toString(),
          writeOutageTimeout);
    }
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
    DROPPED,
    /**
     * Neither dead-lettered nor dropped: the pipeline is stopping, the partition moved to another
     * assignment, or the DLQ failed fatally (the pipeline stops). Leave the records unacked so they
     * are redelivered.
     */
    ABANDONED
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
    return executeWithRetries(task, tp, description, () -> true, () -> true);
  }

  /**
   * Executes a task with retries according to the error strategy.
   *
   * <p>Anything the task throws counts as a failed attempt, including {@link Error}s, except a
   * JVM-fatal error such as {@link OutOfMemoryError} (see {@link FatalErrors#isJvmFatal}), which is
   * rethrown: the JVM may be unusable and the record must not be dropped. An interrupt counts as a
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
    return executeWithRetries(task, tp, description, stopping, () -> true);
  }

  /**
   * Like {@link #executeWithRetries(RetryableTask, TopicPartition, String, BooleanSupplier)}, and
   * stops retrying as soon as this consumer no longer owns the partition in the record's assignment
   * ({@code owned} turns false), returning the last error; the caller then leaves the record for
   * its new owner.
   *
   * @param task the task to execute
   * @param tp the topic partition for logging
   * @param description description for logging
   * @param stopping whether the pipeline is shutting down
   * @param owned whether this consumer still owns the partition in the record's assignment
   * @return null if successful, otherwise the last exception
   */
  public Exception executeWithRetries(
      RetryableTask task,
      TopicPartition tp,
      String description,
      BooleanSupplier stopping,
      BooleanSupplier owned) {
    Exception lastError = null;
    int maxAttempts = 1 + strategy.maxRetries();

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        boolean cutShort = waitOutBackoff(strategy.backoffForAttempt(attempt), owned);
        if (cutShort && stopping.getAsBoolean()) {
          // Shutdown ended the backoff (or interrupted it): leave the record for redelivery.
          Thread.currentThread().interrupt();
          return new InterruptedException("Shutdown during retry backoff");
        }
        Thread.interrupted(); // not a shutdown: an interrupt left over on this thread
        if (!owned.getAsBoolean()) return lastError; // the partition moved on: stop retrying
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
    return Causes.anyMatch(t, c -> c instanceof InterruptedException);
  }

  /**
   * Handles a failure after retries are exhausted, with a pipeline that never stops, a partition
   * that stays owned, and fatal DLQ failures only logged. For tests and simple callers.
   *
   * @param records the failed records
   * @param tp the topic partition
   * @param error the exception that caused failure
   * @param description description for logging
   * @return how the failure was resolved
   */
  public FailureResolution handleFailure(
      List<ConsumerRecord<K, V>> records, TopicPartition tp, Exception error, String description) {
    return handleFailure(
        records,
        tp,
        error,
        description,
        () -> false,
        () -> true,
        t -> logger.log(System.Logger.Level.ERROR, "Fatal DLQ failure", t));
  }

  /**
   * Handles a failure after retries are exhausted. With {@code Fallback.DROP} or no DLQ, the
   * records are dropped. Otherwise each is sent to the DLQ, and a failed send is classified with
   * {@link DLQHandler#classify}:
   *
   * <ul>
   *   <li>{@code RECORD}: this record cannot be dead-lettered; it and the ones after it are dropped
   *       (the final failure handler is called for each).
   *   <li>{@code TRANSIENT}: the DLQ is down for now; the send is retried with backoff until it
   *       works, so records are never dropped because the DLQ is briefly unavailable. If it stays
   *       down longer than {@code writeOutageTimeout}, the pipeline stops.
   *   <li>{@code FATAL}: the pipeline stops ({@code onFatal}).
   * </ul>
   *
   * <p>The result is {@link FailureResolution#ABANDONED} (records left unacked) when the pipeline
   * stops, the partition is no longer {@code owned}, or the DLQ failed fatally.
   *
   * @param records the failed records
   * @param tp the topic partition
   * @param error the exception that caused failure
   * @param description description for logging
   * @param stopping whether the pipeline is shutting down
   * @param owned whether this consumer still owns the partition in the records' assignment
   * @param onFatal stops the pipeline
   * @return how the failure was resolved
   */
  public FailureResolution handleFailure(
      List<ConsumerRecord<K, V>> records,
      TopicPartition tp,
      Exception error,
      String description,
      BooleanSupplier stopping,
      BooleanSupplier owned,
      Consumer<Throwable> onFatal) {
    int recordCount = records.size();
    if (strategy.fallback() != Fallback.DLQ_THEN_DROP || !strategy.hasDlq()) {
      // No DLQ configured, or Fallback.DROP: call the final failure handler for every record.
      for (ConsumerRecord<K, V> record : records) {
        invokeFinalFailureHandler(record, error);
      }
      metrics.recordDropped(recordCount);
      return FailureResolution.DROPPED;
    }

    DLQHandler<K, V> dlq = strategy.dlqHandler();
    int sent = 0;
    long outageStart = 0;
    int transientAttempt = 0;
    while (sent < recordCount) {
      try {
        dlq.send(records.get(sent), error);
        sent++;
        transientAttempt = 0;
        continue;
      } catch (Throwable dlqThrowable) {
        FatalErrors.rethrowIfJvmFatal(dlqThrowable);
        if (stopping.getAsBoolean()
            && (Thread.currentThread().isInterrupted() || causedByInterrupt(dlqThrowable))) {
          return abandon(tp, description, sent, "interrupted by shutdown");
        }
        Exception dlqError = asException(dlqThrowable);
        WriteFailure kind = classify(dlq, dlqError);
        if (kind == WriteFailure.FATAL) {
          onFatal.accept(dlqError);
          return abandon(tp, description, sent, "the DLQ failed fatally: " + dlqError);
        }
        if (kind == WriteFailure.TRANSIENT) {
          if (stopping.getAsBoolean() || !owned.getAsBoolean()) {
            return abandon(tp, description, sent, "stopping or partition revoked");
          }
          long now = System.nanoTime();
          if (transientAttempt == 0) outageStart = now;
          if (now - outageStart > writeOutageTimeout.toNanos()) {
            onFatal.accept(
                new IllegalStateException(
                    "DLQ unavailable for more than " + writeOutageTimeout, dlqError));
            return abandon(tp, description, sent, "the DLQ is unavailable");
          }
          metrics.recordRetry();
          logOutage("DLQ", dlqError);
          BooleanSupplier keepWaiting = () -> !stopping.getAsBoolean() && owned.getAsBoolean();
          waitOutBackoff(transientBackoff(++transientAttempt), keepWaiting);
          if (!keepWaiting.getAsBoolean()) {
            return abandon(tp, description, sent, "stopping or partition revoked");
          }
          Thread.interrupted(); // a leftover interrupt while running is not a shutdown
          continue;
        }
        return dropRest(records, tp, description, sent, dlqError);
      }
    }
    metrics.recordDlqSuccess(recordCount);
    logger.log(System.Logger.Level.INFO, "Sent to DLQ: {0} ({1})", tp, description);
    return FailureResolution.DLQ_SUCCESS;
  }

  private static WriteFailure classify(DLQHandler<?, ?> dlq, Exception error) {
    return Classification.classify(dlq::classify, error);
  }

  private FailureResolution abandon(TopicPartition tp, String description, int sent, String why) {
    if (sent > 0) metrics.recordDlqSuccess(sent);
    logger.log(
        System.Logger.Level.DEBUG,
        "DLQ send for {0} ({1}) abandoned, {2}; leaving it for redelivery",
        tp,
        description,
        why);
    return FailureResolution.ABANDONED;
  }

  /** The DLQ cannot take record {@code sent}: drop it and the records after it. */
  private FailureResolution dropRest(
      List<ConsumerRecord<K, V>> records,
      TopicPartition tp,
      String description,
      int sent,
      Exception dlqError) {
    int recordCount = records.size();
    int failedToSend = recordCount - sent;
    if (sent > 0) {
      metrics.recordDlqSuccess(sent);
      logger.log(
          System.Logger.Level.WARNING,
          "Partial DLQ send for {0} ({1}): {2}/{3} records sent before failure",
          tp,
          description,
          sent,
          recordCount);
    }
    metrics.recordDlqFailure(failedToSend);
    logger.log(
        System.Logger.Level.ERROR,
        "DLQ send failed for {0} ({1}): {2}",
        tp,
        description,
        dlqError.getMessage());
    for (int i = sent; i < recordCount; i++) {
      invokeFinalFailureHandler(records.get(i), dlqError);
    }
    // Only the records that did not reach the DLQ are dropped; the others count as DLQ'd.
    metrics.recordDropped(failedToSend);
    return FailureResolution.DROPPED;
  }

  private void invokeFinalFailureHandler(ConsumerRecord<K, V> record, Exception error) {
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
