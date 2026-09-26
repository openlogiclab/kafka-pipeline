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

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RetryExecutorTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);

  private RetryExecutor<String, String> executor;

  @Nested
  class ExecuteWithRetries {

    @Test
    void successOnFirstAttempt_noRetries() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder().maxRetries(3).build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      AtomicInteger attempts = new AtomicInteger();
      Exception result =
          executor.executeWithRetries(attempt -> attempts.incrementAndGet(), TP0, "test");

      assertNull(result);
      assertEquals(1, attempts.get());
    }

    @Test
    void failsOnFirstAttempt_retriesAndSucceeds() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(3)
              .retryBackoff(Duration.ofMillis(1))
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      AtomicInteger attempts = new AtomicInteger();
      Exception result =
          executor.executeWithRetries(
              attempt -> {
                attempts.incrementAndGet();
                if (attempt < 2) {
                  throw new RuntimeException("fail");
                }
              },
              TP0,
              "test");

      assertNull(result);
      assertEquals(3, attempts.get());
    }

    @Test
    void allRetriesFail_returnsLastError() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(2)
              .retryBackoff(Duration.ofMillis(1))
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      AtomicInteger attempts = new AtomicInteger();
      Exception result =
          executor.executeWithRetries(
              attempt -> {
                attempts.incrementAndGet();
                throw new RuntimeException("fail-" + attempt);
              },
              TP0,
              "test");

      assertNotNull(result);
      assertEquals("fail-2", result.getMessage());
      assertEquals(3, attempts.get());
    }

    @Test
    void interruptedDuringBackoff_returnsInterruptedException() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(3)
              .retryBackoff(Duration.ofSeconds(10))
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      Thread.currentThread().interrupt();
      Exception result =
          executor.executeWithRetries(
              attempt -> {
                throw new RuntimeException("fail");
              },
              TP0,
              "test");

      assertTrue(result instanceof InterruptedException);
      assertTrue(Thread.currentThread().isInterrupted());
      Thread.interrupted();
    }
  }

  @Nested
  class HandleFailure {

    private List<ConsumerRecord<String, String>> records;

    @BeforeEach
    void setUp() {
      records = new ArrayList<>();
      records.add(new ConsumerRecord<>("test", 0, 0L, "k0", "v0"));
      records.add(new ConsumerRecord<>("test", 0, 1L, "k1", "v1"));
      records.add(new ConsumerRecord<>("test", 0, 2L, "k2", "v2"));
    }

    @Test
    void dlqSuccess_returnsDlqSuccess() {
      AtomicInteger dlqCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler((record, error) -> dlqCalls.incrementAndGet())
              .fallback(Fallback.SKIP)
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.DLQ_SUCCESS, result);
      assertEquals(3, dlqCalls.get());
    }

    @Test
    void partialDlqFailure_firstRecordSent_thenFailure() {
      AtomicInteger dlqCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (record, error) -> {
                    int call = dlqCalls.incrementAndGet();
                    if (call > 1) {
                      throw new RuntimeException("DLQ failed");
                    }
                  })
              .fallback(Fallback.SKIP)
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIP, result);
      assertEquals(2, dlqCalls.get());
    }

    @Test
    void dlqFailure_allRecordsFail_fallbackToSkip() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (record, error) -> {
                    throw new RuntimeException("DLQ failed");
                  })
              .fallback(Fallback.SKIP)
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIP, result);
    }

    @Test
    void dlqFailure_fallbackToFailPartition() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (record, error) -> {
                    throw new RuntimeException("DLQ failed");
                  })
              .fallback(Fallback.FAIL_PARTITION)
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.FAIL_PARTITION, result);
    }

    @Test
    void noDlq_fallbackToSkip() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder().fallback(Fallback.SKIP).build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIP, result);
    }

    @Test
    void noDlq_fallbackToFailPartition() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder().fallback(Fallback.FAIL_PARTITION).build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.FAIL_PARTITION, result);
    }
  }
}
