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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureController;
import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.backpressure.RecordCountSensor;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.internal.HandlerAttemptObserver;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import io.github.openlogiclab.kafkapipeline.metrics.DurationStats;
import io.github.openlogiclab.kafkapipeline.metrics.PipelineMetrics;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
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
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.DLQ_SUCCESS, result);
      assertEquals(3, dlqCalls.get());
    }

    @Test
    void partialDlqFailure_firstRecordSent_thenSkip() {
      AtomicInteger dlqCalls = new AtomicInteger();
      AtomicInteger finalFailureCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (record, error) -> {
                    int call = dlqCalls.incrementAndGet();
                    if (call > 1) {
                      throw new RuntimeException("DLQ failed");
                    }
                  })
              .onFinalFailure((record, error) -> finalFailureCalls.incrementAndGet())
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIPPED, result);
      assertEquals(2, dlqCalls.get());
      assertEquals(2, finalFailureCalls.get());
    }

    @Test
    void dlqFailure_allRecordsFail_skipped() {
      AtomicInteger finalFailureCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (record, error) -> {
                    throw new RuntimeException("DLQ failed");
                  })
              .onFinalFailure((record, error) -> finalFailureCalls.incrementAndGet())
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIPPED, result);
      assertEquals(3, finalFailureCalls.get());
    }

    @Test
    void noDlq_callsFinalFailureHandler() {
      AtomicInteger finalFailureCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .onFinalFailure((record, error) -> finalFailureCalls.incrementAndGet())
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIPPED, result);
      assertEquals(3, finalFailureCalls.get());
    }

    @Test
    void finalFailureHandlerException_logsButDoesNotThrow() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .onFinalFailure(
                  (record, error) -> {
                    throw new RuntimeException("Handler failed");
                  })
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result =
          assertDoesNotThrow(
              () -> executor.handleFailure(records, TP0, new RuntimeException("test"), "batch"));

      assertEquals(RetryExecutor.FailureResolution.SKIPPED, result);
    }

    @Test
    void fallbackSkip_skipsDlqEvenWhenConfigured() {
      AtomicInteger dlqCalls = new AtomicInteger();
      AtomicInteger finalFailureCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler((record, error) -> dlqCalls.incrementAndGet())
              .fallback(Fallback.SKIP) // explicit SKIP should bypass DLQ
              .onFinalFailure((record, error) -> finalFailureCalls.incrementAndGet())
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.SKIPPED, result);
      assertEquals(0, dlqCalls.get()); // DLQ should NOT be called
      assertEquals(3, finalFailureCalls.get()); // FinalFailureHandler called for all records
    }

    @Test
    void fallbackDlqThenSkip_usesDlqWhenConfigured() {
      AtomicInteger dlqCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler((record, error) -> dlqCalls.incrementAndGet())
              .fallback(Fallback.DLQ_THEN_SKIP)
              .build();
      executor = new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE);

      var result = executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertEquals(RetryExecutor.FailureResolution.DLQ_SUCCESS, result);
      assertEquals(3, dlqCalls.get()); // DLQ called for all records
    }
  }

  /**
   * Each failed record lands in exactly one outcome counter: {@code dlqSuccesses} or {@code
   * recordsFailed}. {@code recordsSkipped} is reserved for hook skips.
   */
  @Nested
  class OutcomeMetrics {

    private List<ConsumerRecord<String, String>> records;
    private PipelineMetricsCollector collector;

    @BeforeEach
    void setUp() {
      records = new ArrayList<>();
      for (long offset = 0; offset < 3; offset++) {
        records.add(new ConsumerRecord<>("test", 0, offset, "k" + offset, "v" + offset));
      }
      InFlightCounter counter = new InFlightCounter();
      BackpressureConfig bpConfig = BackpressureConfig.builder().build();
      collector =
          new PipelineMetricsCollector(
              "test",
              counter,
              new BackpressureController(bpConfig, new RecordCountSensor(bpConfig, counter)),
              new UnorderedOffsetTracker());
    }

    private void assertOutcomes(long dlq, long failed) {
      PipelineMetrics m = collector.snapshot();
      assertEquals(dlq, m.dlqSuccesses(), "dlqSuccesses");
      assertEquals(failed, m.recordsFailed(), "recordsFailed");
      assertEquals(failed, m.finalFailures(), "finalFailures");
      assertEquals(0, m.recordsSkipped(), "recordsSkipped is for hook skips only");
      assertEquals(0, m.recordsProcessed(), "recordsProcessed");
      assertEquals(
          records.size(), m.dlqSuccesses() + m.recordsFailed(), "each record counted once");
    }

    @Test
    void dlqSuccess_countsOnlyAsDlq() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder().dlqHandler((record, error) -> {}).build();
      executor = new RetryExecutor<>(strategy, collector);

      executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertOutcomes(3, 0);
    }

    @Test
    void partialDlqFailure_splitsBetweenDlqAndFailed() {
      AtomicInteger dlqCalls = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (record, error) -> {
                    if (dlqCalls.incrementAndGet() > 1) throw new RuntimeException("DLQ failed");
                  })
              .build();
      executor = new RetryExecutor<>(strategy, collector);

      executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertOutcomes(1, 2);
      assertEquals(2, collector.snapshot().dlqFailures());
    }

    @Test
    void noDlq_countsAllAsFailed() {
      executor = new RetryExecutor<>(ErrorStrategy.skipOnError(), collector);

      executor.handleFailure(records, TP0, new RuntimeException("test"), "batch");

      assertOutcomes(0, 3);
    }
  }

  @Nested
  class HandlerDuration {

    private PipelineMetricsCollector collector;

    @BeforeEach
    void setUp() {
      InFlightCounter counter = new InFlightCounter();
      BackpressureConfig bpConfig = BackpressureConfig.builder().build();
      collector =
          new PipelineMetricsCollector(
              "test",
              counter,
              new BackpressureController(bpConfig, new RecordCountSensor(bpConfig, counter)),
              new UnorderedOffsetTracker());
    }

    @Test
    void everyAttemptIsTimed_includingFailedOnes() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(2)
              .retryBackoff(Duration.ofMillis(1))
              .build();
      executor = new RetryExecutor<>(strategy, collector, collector, Duration.ZERO);

      Exception result =
          executor.executeWithRetries(
              attempt -> {
                Thread.sleep(5);
                if (attempt < 2) throw new RuntimeException("fail " + attempt);
              },
              TP0,
              "offset 0");

      assertNull(result);
      DurationStats d = collector.snapshot().handlerDuration();
      assertEquals(3, d.count(), "two failed attempts and one success");
      assertTrue(d.total().toMillis() >= 15, "total " + d.total());
      assertTrue(d.mean().toMillis() >= 5, "mean " + d.mean());
    }
  }

  @Nested
  class AttemptOutcomes {

    private final List<AttemptOutcome> outcomes = new ArrayList<>();
    private final HandlerAttemptObserver observer = (tp, nanos, outcome) -> outcomes.add(outcome);

    @Test
    void failedThenSuccessfulAttempt() {
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(1)
              .retryBackoff(Duration.ofMillis(1))
              .build();
      executor =
          new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE, observer, Duration.ZERO);

      executor.executeWithRetries(
          attempt -> {
            if (attempt == 0) throw new RuntimeException("first fails");
          },
          TP0,
          "offset 0");

      assertEquals(List.of(AttemptOutcome.FAILURE, AttemptOutcome.SUCCESS), outcomes);
    }

    @Test
    void attemptPastProcessingTimeoutIsTimeout() {
      executor =
          new RetryExecutor<>(
              ErrorStrategy.skipOnError(),
              NoOpMetricsCollector.INSTANCE,
              observer,
              Duration.ofMillis(50));
      try {
        executor.executeWithRetries(attempt -> Thread.sleep(5_000), TP0, "offset 0", () -> false);
      } finally {
        executor.close();
      }

      assertEquals(List.of(AttemptOutcome.TIMEOUT), outcomes);
    }
  }
}
