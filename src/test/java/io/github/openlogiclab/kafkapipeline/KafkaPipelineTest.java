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
import io.github.openlogiclab.kafkapipeline.backpressure.HeapBackpressureConfig;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaPipelineTest {

  private static final String TOPIC = "unit-test";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);

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
        .concurrency(ThreadMode.PLATFORM, 2)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(50))
        .shutdownTimeout(Duration.ofSeconds(5));
  }

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

  private boolean awaitCondition(java.util.function.BooleanSupplier cond, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }

  @Nested
  class LifecycleBranches {

    @Test
    void doubleStartThrows() throws Exception {
      PipelineConfig<String, String> config = baseBuilder().handler(record -> {}).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      pipeline.startAsync();
      Thread.sleep(100);

      assertThrows(IllegalStateException.class, () -> pipeline.start());
    }

    @Test
    void stopBeforeStartIsNoOp() {
      PipelineConfig<String, String> config = baseBuilder().handler(record -> {}).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      assertFalse(pipeline.isRunning());
      pipeline.stop();
      assertFalse(pipeline.isRunning());
    }

    @Test
    void doubleStopIsNoOp() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> processed.add(record.value())).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      startAndAssign(TP0);
      pipeline.stop();
      pipeline.stop();
      pipeline.awaitShutdown();
      assertFalse(pipeline.isRunning());
    }

    @Test
    void isRunningReflectsPipelineState() throws Exception {
      PipelineConfig<String, String> config = baseBuilder().handler(record -> {}).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      assertFalse(pipeline.isRunning());

      startAndAssign(TP0);
      assertTrue(pipeline.isRunning());

      pipeline.stop();
      pipeline.awaitShutdown();
      assertFalse(pipeline.isRunning());
    }
  }

  @Nested
  class PollLoopBranches {

    @Test
    void dispatchQueueFull_blocksUntilSpaceAvailable() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      CountDownLatch blockLatch = new CountDownLatch(1);

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 1)
              .dispatchQueueCapacity(2)
              .handler(
                  record -> {
                    if (record.offset() == 0L) {
                      try {
                        blockLatch.await(10, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }
                    processed.add(record.value());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      for (int i = 0; i < 5; i++) {
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "value-" + i));
      }

      Thread.sleep(500);
      blockLatch.countDown();

      assertTrue(
          awaitCondition(() -> processed.size() == 5, Duration.ofSeconds(5)),
          "All records should eventually be processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();
      assertEquals(5, processed.size(), "No records should be dropped with blocking dispatch");
    }

    @Test
    void emptyPollReturnsNoRecords() throws Exception {
      AtomicInteger handlerCalls = new AtomicInteger();
      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> handlerCalls.incrementAndGet()).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      startAndAssign(TP0);
      Thread.sleep(200);

      pipeline.stop();
      pipeline.awaitShutdown();
      assertEquals(0, handlerCalls.get());
    }

    @Test
    void wakeupExceptionWhileRunningIsTolerated() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> processed.add(record.value())).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      startAndAssign(TP0);

      Thread.sleep(100);
      mockConsumer.wakeup();
      Thread.sleep(100);

      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "after-wakeup"));
      assertTrue(awaitCondition(() -> processed.size() == 1, Duration.ofSeconds(3)));

      pipeline.stop();
      pipeline.awaitShutdown();
      assertEquals("after-wakeup", processed.getFirst());
    }

    @Test
    void generalExceptionInPollLoopDoesNotCrash() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder().handler(record -> processed.add(record.value())).build();
      pipeline = new KafkaPipeline<>(config, mockConsumer);

      startAndAssign(TP0);

      mockConsumer.setPollException(new KafkaException("Transient poll error"));
      Thread.sleep(300);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "recovered"));
      assertTrue(
          awaitCondition(() -> processed.size() == 1, Duration.ofSeconds(3)),
          "Pipeline should recover after transient poll exception");

      pipeline.stop();
      pipeline.awaitShutdown();
    }

    @Test
    void backpressurePausesAndResumesConsumer() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      CountDownLatch blockLatch = new CountDownLatch(1);
      int total = 20;

      PipelineConfig<String, String> config =
          baseBuilder()
              .concurrency(ThreadMode.PLATFORM, 1)
              .backpressure(
                  BackpressureConfig.builder()
                      .highWatermark(5)
                      .lowWatermark(2)
                      .criticalThreshold(15)
                      .build())
              .handler(
                  record -> {
                    if (record.offset() == 0L) {
                      try {
                        blockLatch.await(5, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }
                    processed.add(record.value());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      for (int i = 0; i < total; i++) {
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "val-" + i));
      }

      Thread.sleep(300);
      blockLatch.countDown();

      assertTrue(
          awaitCondition(() -> processed.size() == total, Duration.ofSeconds(10)),
          "Expected " + total + " processed, got " + processed.size());

      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  @Nested
  class OwnsConsumerShutdown {

    @Test
    void ownsConsumer_closesConsumerOnShutdown() throws Exception {
      PipelineConfig<String, String> config = baseBuilder().handler(record -> {}).build();

      pipeline = new KafkaPipeline<>(config, mockConsumer, true);
      startAndAssign(TP0);

      pipeline.stop();
      pipeline.awaitShutdown();

      assertFalse(pipeline.isRunning());
      assertTrue(mockConsumer.closed(), "Consumer should be closed when ownsConsumer=true");
    }
  }

  @Nested
  class HeapBackpressureIntegration {

    @Test
    void heapBackpressureEnabled_processesRecords() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .handler(record -> processed.add(record.value()))
              .heapBackpressure(HeapBackpressureConfig.builder().build())
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);

      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "heap-ok"));
      assertTrue(
          awaitCondition(() -> processed.size() == 1, Duration.ofSeconds(3)),
          "Record should be processed with heap backpressure enabled");

      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  @Nested
  class ShutdownErrorPaths {

    @Test
    void shutdownHandlesWorkerPoolStopError() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      PipelineConfig<String, String> config =
          baseBuilder()
              .shutdownTimeout(Duration.ofMillis(1))
              .handler(
                  record -> {
                    Thread.sleep(2000);
                    processed.add(record.value());
                  })
              .build();

      pipeline = new KafkaPipeline<>(config, mockConsumer);
      startAndAssign(TP0);
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "slow"));
      Thread.sleep(200);

      pipeline.stop();
      pipeline.awaitShutdown();
      assertFalse(pipeline.isRunning());
    }
  }
}
