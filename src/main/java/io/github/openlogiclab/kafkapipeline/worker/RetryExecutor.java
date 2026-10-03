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
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import java.util.List;
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
  private final PipelineMetricsCollector metricsCollector;

  /**
   * Creates a retry executor.
   *
   * @param strategy the error strategy
   * @param metricsCollector the metrics collector
   */
  public RetryExecutor(ErrorStrategy<K, V> strategy, PipelineMetricsCollector metricsCollector) {
    this.strategy = strategy;
    this.metricsCollector = metricsCollector;
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
    /** Records were skipped. */
    SKIPPED
  }

  /**
   * Executes a task with retries according to the error strategy.
   *
   * @param task the task to execute
   * @param tp the topic partition for logging
   * @param description description for logging
   * @return null if successful, otherwise the last exception
   */
  public Exception executeWithRetries(RetryableTask task, TopicPartition tp, String description) {
    Exception lastError = null;
    int maxAttempts = 1 + strategy.maxRetries();

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        if (attempt > 0) {
          Thread.sleep(strategy.backoffForAttempt(attempt).toMillis());
        }
        task.execute(attempt);
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return e;
      } catch (Exception e) {
        lastError = e;
        if (attempt < maxAttempts - 1) {
          metricsCollector.recordRetry();
          logger.log(
              System.Logger.Level.DEBUG,
              "Retry {0}/{1} for {2}: {3}",
              attempt + 1,
              strategy.maxRetries(),
              tp,
              description);
        }
      }
    }
    return lastError;
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
        metricsCollector.recordDlqSuccess(recordCount);
        logger.log(System.Logger.Level.INFO, "Sent to DLQ: {0} ({1})", tp, description);
        return FailureResolution.DLQ_SUCCESS;
      } catch (Exception dlqError) {
        int failedToSend = recordCount - sentToDlq;
        if (sentToDlq > 0) {
          metricsCollector.recordDlqSuccess(sentToDlq);
          logger.log(
              System.Logger.Level.WARNING,
              "Partial DLQ send for {0} ({1}): {2}/{3} records sent before failure",
              tp,
              description,
              sentToDlq,
              recordCount);
        }
        metricsCollector.recordDlqFailure(failedToSend);
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
      }
    } else {
      // No DLQ configured, call final failure handler for all records
      for (ConsumerRecord<K, V> record : records) {
        invokeFinalFailureHandler(record, error);
      }
    }

    metricsCollector.recordSkipped(recordCount);
    return FailureResolution.SKIPPED;
  }

  private void invokeFinalFailureHandler(ConsumerRecord<K, V> record, Exception error) {
    metricsCollector.recordFinalFailure();
    try {
      strategy.finalFailureHandler().handle(record, error);
    } catch (Exception handlerError) {
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
