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
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Unified retry → DLQ → fallback engine shared by both per-record and batch processing paths.
 *
 * <p>Stateless — all state lives in the caller or in {@link ErrorStrategy}. Thread-safe as long as
 * the {@link ErrorStrategy} and its {@link DLQHandler} are thread-safe.
 */
public final class RetryExecutor<K, V> {

  private static final System.Logger logger = System.getLogger(RetryExecutor.class.getName());

  private final ErrorStrategy<K, V> strategy;

  public RetryExecutor(ErrorStrategy<K, V> strategy) {
    this.strategy = strategy;
  }

  @FunctionalInterface
  public interface RetryableTask {
    void execute(int attempt) throws Exception;
  }

  public enum FailureResolution {
    DLQ_SUCCESS,
    SKIP,
    FAIL_PARTITION
  }

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

  public FailureResolution handleFailure(
      List<ConsumerRecord<K, V>> records, TopicPartition tp, Exception error, String description) {

    if (strategy.hasDlq()) {
      try {
        DLQHandler<K, V> dlq = strategy.dlqHandler();
        for (ConsumerRecord<K, V> record : records) {
          dlq.send(record, error);
        }
        logger.log(System.Logger.Level.INFO, "Sent to DLQ: {0} ({1})", tp, description);
        return FailureResolution.DLQ_SUCCESS;
      } catch (Exception dlqError) {
        logger.log(
            System.Logger.Level.ERROR,
            "DLQ send failed for {0} ({1}): {2}",
            tp,
            description,
            dlqError.getMessage());
      }
    }

    if (strategy.fallback() == Fallback.SKIP) {
      logger.log(
          System.Logger.Level.WARNING,
          "Skipping failed {0} ({1}): {2}",
          tp,
          description,
          error.getMessage());
      return FailureResolution.SKIP;
    }

    logger.log(
        System.Logger.Level.ERROR,
        "Failing partition {0} due to {1}: {2}",
        tp,
        description,
        error.getMessage());
    return FailureResolution.FAIL_PARTITION;
  }
}
