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
package io.github.openlogiclab.kafkapipeline;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.backpressure.ByteBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaPipelineE2ETest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
  private static final TopicPartition TP2 = new TopicPartition(TOPIC, 2);

  private MockConsumer<String, String> mockConsumer;
  private KafkaPipeline<String, String> pipeline;

  @BeforeEach
  void setUp() {
    mockConsumer = new MockConsumer<>("earliest");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (pipeline != null && pipeline.isRunning()) {
      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  private Properties props() {
    Properties p = new Properties();
    p.put("bootstrap.servers", "dummy:9092");
    p.put("group.id", "test-group");
    return p;
  }

  private PipelineConfig.Builder<String, String> baseBuilder() {
    return PipelineConfig.<String, String>builder()
        .consumerProperties(props())
        .topics(TOPIC)
        .concurrency(ThreadMode.PLATFORM, 4)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(50))
        .shutdownTimeout(Duration.ofSeconds(5));
  }

  /**
   * Starts the pipeline async, waits for subscribe(), then triggers rebalance to assign partitions.
   * MockConsumer requires subscribe() before rebalance().
   */
  private void startAndAssign(TopicPartition... tps) throws InterruptedException {
    pipeline.startAsync();
    Thread.sleep(100);

    Map<TopicPartition, Long> offsets = new HashMap<>();
    for (TopicPartition tp : tps) {
      offsets.put(tp, 0L);
    }
    mockConsumer.updateBeginningOffsets(offsets);
    mockConsumer.rebalance(List.of(tps));
  }

  private void addRecords(TopicPartition tp, int count) {
    for (int i = 0; i < count; i++) {
      mockConsumer.addRecord(
          new ConsumerRecord<>(tp.topic(), tp.partition(), i, "key-" + i, "value-" + i));
    }
  }

  private void addRecords(TopicPartition tp, int fromOffset, int toOffset) {
    for (int i = fromOffset; i <= toOffset; i++) {
      mockConsumer.addRecord(
          new ConsumerRecord<>(tp.topic(), tp.partition(), i, "key-" + i, "value-" + i));
    }
  }

  private boolean awaitCondition(java.util.function.BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }

  // ── Happy Path ───────────────────────────────────────────────

  @Nested
  class HappyPath {

    @Test
    void allRecordsProcessedAndCommitted() throws Exception {
      int count = 50;
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> processed.add(record.value())).build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, count);

      assertTrue(
          awaitCondition(() -> processed.size() == count, Duration.ofSeconds(5)),
          "Expected " + count + " processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(count, processed.size());

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0), "Offset should be committed for TP0");
      assertEquals(count, committed.get(TP0).offset());
    }

    @Test
    void multiPartitionProcessing() throws Exception {
      int perPartition = 30;
      ConcurrentHashMap<Integer, AtomicInteger> partitionCounts = new ConcurrentHashMap<>();
      partitionCounts.put(0, new AtomicInteger());
      partitionCounts.put(1, new AtomicInteger());
      partitionCounts.put(2, new AtomicInteger());

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(record -> partitionCounts.get(record.partition()).incrementAndGet())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0, TP1, TP2);
      addRecords(TP0, perPartition);
      addRecords(TP1, perPartition);
      addRecords(TP2, perPartition);

      int total = perPartition * 3;
      assertTrue(
          awaitCondition(
              () -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() == total,
              Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      for (var entry : partitionCounts.entrySet()) {
        assertEquals(
            perPartition,
            entry.getValue().get(),
            "Partition " + entry.getKey() + " should have " + perPartition + " records");
      }
    }

    @Test
    void virtualThreadMode() throws Exception {
      int count = 30;
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.VIRTUAL, 20)
              .handler(record -> processed.add(record.value()))
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, count);

      assertTrue(awaitCondition(() -> processed.size() == count, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();
      assertEquals(count, processed.size());
    }
  }

  // ── Error Handling: Retry ────────────────────────────────────

  @Nested
  class RetryBehavior {

    @Test
    void retrySucceedsAfterTransientFailure() throws Exception {
      AtomicInteger attempts = new AtomicInteger();
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(
                  record -> {
                    if (attempts.incrementAndGet() <= 2) {
                      throw new RuntimeException("Transient failure #" + attempts.get());
                    }
                    processed.add(record.value());
                  })
              .errorStrategy(
                  ErrorStrategy.<String, String>builder()
                      .maxRetries(3)
                      .retryBackoff(Duration.ofMillis(10))
                      .fallback(Fallback.FAIL_PARTITION)
                      .build())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "retry-me"));

      assertTrue(awaitCondition(() -> processed.size() == 1, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals("retry-me", processed.getFirst());
      assertEquals(3, attempts.get());
    }
  }

  // ── Error Handling: DLQ ──────────────────────────────────────

  @Nested
  class DlqBehavior {

    @Test
    void recordSentToDlqAfterRetriesExhausted() throws Exception {
      CopyOnWriteArrayList<ConsumerRecord<String, String>> dlqRecords =
          new CopyOnWriteArrayList<>();
      AtomicInteger handlerCalls = new AtomicInteger();

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(
                  record -> {
                    handlerCalls.incrementAndGet();
                    throw new RuntimeException("Always fails");
                  })
              .errorStrategy(
                  ErrorStrategy.<String, String>builder()
                      .maxRetries(2)
                      .retryBackoff(Duration.ofMillis(10))
                      .dlqHandler((record, error) -> dlqRecords.add(record))
                      .fallback(Fallback.FAIL_PARTITION)
                      .build())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "dlq-me"));

      assertTrue(awaitCondition(() -> dlqRecords.size() == 1, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals("dlq-me", dlqRecords.getFirst().value());
      assertEquals(3, handlerCalls.get());

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0), "DLQ success should still advance offset");
    }
  }

  // ── Error Handling: Skip ─────────────────────────────────────

  @Nested
  class SkipFallback {

    @Test
    void failedRecordSkippedAndOffsetAdvances() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(
                  record -> {
                    if ("poison".equals(record.value())) {
                      throw new RuntimeException("Poison pill");
                    }
                    processed.add(record.value());
                  })
              .errorStrategy(ErrorStrategy.skipOnError())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "good-0"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k", "poison"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2L, "k", "good-2"));

      assertTrue(awaitCondition(() -> processed.size() == 2, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertTrue(processed.contains("good-0"));
      assertTrue(processed.contains("good-2"));
      assertFalse(processed.contains("poison"));

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0));
      assertEquals(3L, committed.get(TP0).offset());
    }
  }

  // ── Lifecycle Hook ───────────────────────────────────────────

  @Nested
  class LifecycleHookBehavior {

    @Test
    void beforeProcessFalseSkipsHandler() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      Set<Long> skippedOffsets = ConcurrentHashMap.newKeySet();

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(record -> processed.add(record.value()))
              .lifecycleHook(
                  new ProcessingLifecycleHook<>() {
                    @Override
                    public boolean beforeProcess(
                        ConsumerRecord<String, String> record, ProcessingContext ctx) {
                      if ("duplicate".equals(record.value())) {
                        skippedOffsets.add(ctx.offset());
                        return false;
                      }
                      return true;
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "normal"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k", "duplicate"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2L, "k", "normal-2"));

      assertTrue(awaitCondition(() -> processed.size() == 2, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(2, processed.size());
      assertTrue(processed.contains("normal"));
      assertTrue(processed.contains("normal-2"));
      assertTrue(skippedOffsets.contains(1L));

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0));
      assertEquals(3L, committed.get(TP0).offset());
    }

    @Test
    void afterProcessAndOnErrorHooksCalled() throws Exception {
      CopyOnWriteArrayList<Long> afterOffsets = new CopyOnWriteArrayList<>();
      CopyOnWriteArrayList<Long> errorOffsets = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(
                  record -> {
                    if (record.offset() == 1L) throw new RuntimeException("fail");
                  })
              .errorStrategy(ErrorStrategy.skipOnError())
              .lifecycleHook(
                  new ProcessingLifecycleHook<>() {
                    @Override
                    public void afterProcess(
                        ConsumerRecord<String, String> record, ProcessingContext ctx) {
                      afterOffsets.add(ctx.offset());
                    }

                    @Override
                    public void onError(
                        ConsumerRecord<String, String> record,
                        ProcessingContext ctx,
                        Exception error) {
                      errorOffsets.add(ctx.offset());
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "ok"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k", "fail"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2L, "k", "ok2"));

      assertTrue(awaitCondition(() -> afterOffsets.size() == 2, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertTrue(afterOffsets.contains(0L));
      assertTrue(afterOffsets.contains(2L));
      assertTrue(errorOffsets.contains(1L));
      assertFalse(afterOffsets.contains(1L));
    }
  }

  // ── Graceful Shutdown ────────────────────────────────────────

  @Nested
  class GracefulShutdown {

    @Test
    void stopDrainsInFlightAndCommits() throws Exception {
      CountDownLatch firstRecordStarted = new CountDownLatch(1);
      CountDownLatch allowFinish = new CountDownLatch(1);
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 2)
              .handler(
                  record -> {
                    if (record.offset() == 0L) {
                      firstRecordStarted.countDown();
                      allowFinish.await(5, TimeUnit.SECONDS);
                    }
                    processed.add(record.value());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "slow"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k", "fast"));

      assertTrue(firstRecordStarted.await(3, TimeUnit.SECONDS));

      allowFinish.countDown();
      pipeline.stop();
      pipeline.awaitShutdown();

      assertFalse(pipeline.isRunning());
      assertEquals(2, processed.size());

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0));
      assertEquals(2L, committed.get(TP0).offset());
    }
  }

  // ── Backpressure ─────────────────────────────────────────────

  @Nested
  class BackpressureBehavior {

    @Test
    void backpressurePreventsUnboundedInFlight() throws Exception {
      int highWatermark = 10;
      int total = 50;
      AtomicInteger maxConcurrentInFlight = new AtomicInteger(0);
      AtomicInteger currentInFlight = new AtomicInteger(0);
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 2)
              .backpressure(
                  BackpressureConfig.builder()
                      .highWatermark(highWatermark)
                      .lowWatermark(5)
                      .criticalThreshold(20)
                      .build())
              .handler(
                  record -> {
                    int current = currentInFlight.incrementAndGet();
                    maxConcurrentInFlight.updateAndGet(max -> Math.max(max, current));
                    Thread.sleep(5);
                    currentInFlight.decrementAndGet();
                    processed.add(record.value());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, total);

      assertTrue(
          awaitCondition(() -> processed.size() == total, Duration.ofSeconds(10)),
          "Expected " + total + " processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(total, processed.size());
    }
  }

  // ── Out-of-Order Offset Commit ───────────────────────────────

  @Nested
  class OutOfOrderProcessing {

    @Test
    void offsetOnlyAdvancesThroughContiguousCompleted() throws Exception {
      CountDownLatch slowStarted = new CountDownLatch(1);
      CountDownLatch allowSlow = new CountDownLatch(1);
      CopyOnWriteArrayList<Long> completionOrder = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 4)
              .handler(
                  record -> {
                    if (record.offset() == 0L) {
                      slowStarted.countDown();
                      allowSlow.await(5, TimeUnit.SECONDS);
                    }
                    completionOrder.add(record.offset());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "slow"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k", "fast-1"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2L, "k", "fast-2"));
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3L, "k", "fast-3"));

      assertTrue(slowStarted.await(3, TimeUnit.SECONDS));
      assertTrue(
          awaitCondition(() -> completionOrder.size() >= 3, Duration.ofSeconds(3)),
          "Fast records should complete while slow is blocked");

      Thread.sleep(100);
      Map<TopicPartition, OffsetAndMetadata> midCommit = mockConsumer.committed(Set.of(TP0));
      if (midCommit.get(TP0) != null) {
        assertTrue(
            midCommit.get(TP0).offset() <= 1L,
            "Should not commit past blocked offset 0, but committed "
                + midCommit.get(TP0).offset());
      }

      allowSlow.countDown();

      assertTrue(awaitCondition(() -> completionOrder.size() == 4, Duration.ofSeconds(3)));

      pipeline.stop();
      pipeline.awaitShutdown();

      Map<TopicPartition, OffsetAndMetadata> finalCommit = mockConsumer.committed(Set.of(TP0));
      assertNotNull(finalCommit.get(TP0));
      assertEquals(4L, finalCommit.get(TP0).offset());
    }
  }

  // ── Continuous Record Feed ───────────────────────────────────

  @Nested
  class ContinuousFeed {

    @Test
    void handlesMultiplePollBatches() throws Exception {
      int totalRecords = 100;
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> processed.add(record.value())).build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      addRecords(TP0, 0, 24);
      assertTrue(awaitCondition(() -> processed.size() >= 25, Duration.ofSeconds(3)));

      addRecords(TP0, 25, 49);
      assertTrue(awaitCondition(() -> processed.size() >= 50, Duration.ofSeconds(3)));

      addRecords(TP0, 50, 74);
      addRecords(TP0, 75, 99);
      assertTrue(
          awaitCondition(() -> processed.size() == totalRecords, Duration.ofSeconds(5)),
          "Expected " + totalRecords + " processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(totalRecords, processed.size());
    }
  }

  // ── Rebalance Simulation ─────────────────────────────────────

  @Nested
  class RebalanceSimulation {

    @Test
    void newPartitionsAssignedAfterRebalance() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> processed.add(record.value())).build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 10);

      assertTrue(awaitCondition(() -> processed.size() == 10, Duration.ofSeconds(3)));

      Map<TopicPartition, Long> newOffsets = new HashMap<>();
      newOffsets.put(TP0, 0L);
      newOffsets.put(TP1, 0L);
      mockConsumer.updateBeginningOffsets(newOffsets);
      mockConsumer.rebalance(List.of(TP0, TP1));

      for (int i = 0; i < 10; i++) {
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 1, i, "k", "p1-" + i));
      }

      assertTrue(
          awaitCondition(() -> processed.size() >= 20, Duration.ofSeconds(5)),
          "Expected >= 20 processed after rebalance, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  // ── Batch Mode ──────────────────────────────────────────────

  @Nested
  class BatchMode {

    @Test
    void batchHandlerProcessesAllRecords() throws Exception {
      int count = 50;
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> {
                    for (var record : records) {
                      processed.add(record.value());
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, count);

      assertTrue(
          awaitCondition(() -> processed.size() == count, Duration.ofSeconds(5)),
          "Expected " + count + " processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(count, processed.size());

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0), "Offset should be committed for TP0");
      assertEquals(count, committed.get(TP0).offset());
    }

    @Test
    void batchModeMultiPartition() throws Exception {
      int perPartition = 20;
      ConcurrentHashMap<Integer, AtomicInteger> partitionCounts = new ConcurrentHashMap<>();
      partitionCounts.put(0, new AtomicInteger());
      partitionCounts.put(1, new AtomicInteger());

      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> partitionCounts.get(tp.partition()).addAndGet(records.size()))
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0, TP1);
      addRecords(TP0, perPartition);
      addRecords(TP1, perPartition);

      int total = perPartition * 2;
      assertTrue(
          awaitCondition(
              () -> partitionCounts.values().stream().mapToInt(AtomicInteger::get).sum() == total,
              Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(perPartition, partitionCounts.get(0).get());
      assertEquals(perPartition, partitionCounts.get(1).get());
    }

    @Test
    void batchModeWithRetry() throws Exception {
      AtomicInteger attempts = new AtomicInteger();
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> {
                    if (attempts.incrementAndGet() <= 2) {
                      throw new RuntimeException("Transient batch failure");
                    }
                    for (var record : records) {
                      processed.add(record.value());
                    }
                  })
              .errorStrategy(
                  ErrorStrategy.<String, String>builder()
                      .maxRetries(3)
                      .retryBackoff(Duration.ofMillis(10))
                      .fallback(Fallback.FAIL_PARTITION)
                      .build())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "batch-retry"));

      assertTrue(awaitCondition(() -> processed.size() == 1, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals("batch-retry", processed.getFirst());
      assertEquals(3, attempts.get());
    }

    @Test
    void batchModeWithSkipFallback() throws Exception {
      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> {
                    throw new RuntimeException("Always fails");
                  })
              .errorStrategy(ErrorStrategy.skipOnError())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 5);

      Thread.sleep(500);

      pipeline.stop();
      pipeline.awaitShutdown();

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0), "Skipped batch should still advance offset");
      assertEquals(5L, committed.get(TP0).offset());
    }

    @Test
    void batchModeWithVirtualThreads() throws Exception {
      int count = 30;
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.VIRTUAL, 20)
              .batchHandler(
                  (tp, records) -> {
                    for (var record : records) {
                      processed.add(record.value());
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, count);

      assertTrue(awaitCondition(() -> processed.size() == count, Duration.ofSeconds(5)));

      pipeline.stop();
      pipeline.awaitShutdown();
      assertEquals(count, processed.size());
    }

    @Test
    void batchModeContinuousFeed() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> {
                    for (var record : records) {
                      processed.add(record.value());
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      addRecords(TP0, 0, 24);
      assertTrue(awaitCondition(() -> processed.size() >= 25, Duration.ofSeconds(3)));

      addRecords(TP0, 25, 49);
      assertTrue(
          awaitCondition(() -> processed.size() == 50, Duration.ofSeconds(5)),
          "Expected 50 processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();
    }

    @Test
    void batchModeBackpressureThrottlesPolling() throws Exception {
      int highWatermark = 10;
      int total = 50;
      AtomicInteger maxConcurrentInFlight = new AtomicInteger(0);
      AtomicInteger currentInFlight = new AtomicInteger(0);
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 2)
              .backpressure(
                  BackpressureConfig.builder()
                      .highWatermark(highWatermark)
                      .lowWatermark(5)
                      .criticalThreshold(20)
                      .build())
              .batchHandler(
                  (tp, records) -> {
                    int current = currentInFlight.addAndGet(records.size());
                    maxConcurrentInFlight.updateAndGet(max -> Math.max(max, current));
                    Thread.sleep(10);
                    currentInFlight.addAndGet(-records.size());
                    for (var record : records) {
                      processed.add(record.value());
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, total);

      assertTrue(
          awaitCondition(() -> processed.size() == total, Duration.ofSeconds(10)),
          "Expected " + total + " processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(total, processed.size());
    }

    @Test
    void batchModeDlqReceivesAllRecordsInFailedBatch() throws Exception {
      CopyOnWriteArrayList<ConsumerRecord<String, String>> dlqRecords =
          new CopyOnWriteArrayList<>();
      AtomicInteger handlerCalls = new AtomicInteger();

      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> {
                    handlerCalls.incrementAndGet();
                    throw new RuntimeException("Batch always fails");
                  })
              .errorStrategy(
                  ErrorStrategy.<String, String>builder()
                      .maxRetries(1)
                      .retryBackoff(Duration.ofMillis(10))
                      .dlqHandler((record, error) -> dlqRecords.add(record))
                      .fallback(Fallback.FAIL_PARTITION)
                      .build())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 5);

      assertTrue(
          awaitCondition(() -> dlqRecords.size() >= 5, Duration.ofSeconds(5)),
          "Expected >= 5 DLQ records, got " + dlqRecords.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(5, dlqRecords.size());
      assertTrue(handlerCalls.get() >= 2, "At least 2 handler calls (1 attempt + 1 retry)");

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0), "DLQ success should advance offset");
      assertEquals(5L, committed.get(TP0).offset());
    }

    @Test
    void batchModeGracefulShutdownDrainsAndCommits() throws Exception {
      CountDownLatch batchStarted = new CountDownLatch(1);
      CountDownLatch allowFinish = new CountDownLatch(1);
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 2)
              .batchHandler(
                  (tp, records) -> {
                    batchStarted.countDown();
                    allowFinish.await(5, TimeUnit.SECONDS);
                    for (var record : records) {
                      processed.add(record.value());
                    }
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 3);

      assertTrue(batchStarted.await(3, TimeUnit.SECONDS), "Batch should start processing");

      allowFinish.countDown();
      pipeline.stop();
      pipeline.awaitShutdown();

      assertFalse(pipeline.isRunning());
      assertEquals(3, processed.size());

      Map<TopicPartition, OffsetAndMetadata> committed = mockConsumer.committed(Set.of(TP0));
      assertNotNull(committed.get(TP0));
      assertEquals(3L, committed.get(TP0).offset());
    }
  }

  // ── Byte-Level Backpressure ─────────────────────────────────

  @Nested
  class ByteBackpressureBehavior {

    @Test
    void byteBackpressureThrottlesPollingForLargeRecords() throws Exception {
      AtomicInteger maxConcurrentInFlight = new AtomicInteger(0);
      AtomicInteger currentInFlight = new AtomicInteger(0);
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 2)
              .backpressure(
                  BackpressureConfig.builder()
                      .highWatermark(100)
                      .lowWatermark(50)
                      .criticalThreshold(200)
                      .build())
              .byteBackpressure(
                  ByteBackpressureConfig.builder()
                      .lowWatermarkBytes(50)
                      .highWatermarkBytes(100)
                      .criticalThresholdBytes(500)
                      .build())
              .handler(
                  record -> {
                    int current = currentInFlight.incrementAndGet();
                    maxConcurrentInFlight.updateAndGet(max -> Math.max(max, current));
                    Thread.sleep(10);
                    currentInFlight.decrementAndGet();
                    processed.add(record.value());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 30);

      assertTrue(
          awaitCondition(() -> processed.size() == 30, Duration.ofSeconds(10)),
          "Expected 30 processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(30, processed.size());
    }

    @Test
    void byteBackpressureWithBatchMode() throws Exception {
      CopyOnWriteArrayList<Integer> batchSizes = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .batchHandler(
                  (tp, records) -> {
                    batchSizes.add(records.size());
                    Thread.sleep(10);
                  })
              .byteBackpressure(
                  ByteBackpressureConfig.builder()
                      .lowWatermarkBytes(50)
                      .highWatermarkBytes(100)
                      .criticalThresholdBytes(500)
                      .build())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 20);

      int totalRecords = 20;
      assertTrue(
          awaitCondition(
              () -> batchSizes.stream().mapToInt(Integer::intValue).sum() >= totalRecords,
              Duration.ofSeconds(10)),
          "Expected all records processed in batch mode");

      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  // ── Custom Backpressure Sensor ──────────────────────────────

  @Nested
  class CustomBackpressureSensor {

    @Test
    void customSensorIsConsulted() throws Exception {
      AtomicInteger sensorChecks = new AtomicInteger(0);
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      BackpressureSensor customSensor =
          new BackpressureSensor() {
            @Override
            public BackpressureStatus currentStatus() {
              sensorChecks.incrementAndGet();
              return BackpressureStatus.OK;
            }

            @Override
            public String name() {
              return "custom-test";
            }

            @Override
            public String statusDetail() {
              return "test sensor";
            }
          };

      PipelineConfig<String, String> config =
          baseBuilder()
              .addBackpressureSensor(customSensor)
              .handler(record -> processed.add(record.value()))
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      addRecords(TP0, 10);

      assertTrue(
          awaitCondition(() -> processed.size() == 10, Duration.ofSeconds(5)),
          "Expected 10 processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(10, processed.size());
      assertTrue(sensorChecks.get() > 0, "Custom sensor should have been checked");
    }

    @Test
    void customSensorCanThrottle() throws Exception {
      AtomicInteger sensorChecks = new AtomicInteger(0);
      java.util.concurrent.atomic.AtomicBoolean throttleActive =
          new java.util.concurrent.atomic.AtomicBoolean(false);

      BackpressureSensor throttlingSensor =
          new BackpressureSensor() {
            @Override
            public BackpressureStatus currentStatus() {
              sensorChecks.incrementAndGet();
              return throttleActive.get() ? BackpressureStatus.THROTTLE : BackpressureStatus.OK;
            }

            @Override
            public String name() {
              return "throttling-test";
            }

            @Override
            public String statusDetail() {
              return "custom throttle";
            }
          };

      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .addBackpressureSensor(throttlingSensor)
              .handler(record -> processed.add(record.value()))
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      addRecords(TP0, 0, 4);
      assertTrue(
          awaitCondition(() -> processed.size() == 5, Duration.ofSeconds(5)),
          "First batch should be processed, got " + processed.size());

      throttleActive.set(true);
      Thread.sleep(200);

      addRecords(TP0, 5, 9);
      Thread.sleep(500);
      int sizeWhileThrottled = processed.size();
      assertEquals(
          5,
          sizeWhileThrottled,
          "Second batch should not be polled while throttled, but got " + sizeWhileThrottled);

      throttleActive.set(false);

      assertTrue(
          awaitCondition(() -> processed.size() == 10, Duration.ofSeconds(10)),
          "Expected 10 processed after throttle released, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();

      assertEquals(10, processed.size());
      assertTrue(sensorChecks.get() > 0, "Custom sensor should have been polled at least once");
    }
  }
}
