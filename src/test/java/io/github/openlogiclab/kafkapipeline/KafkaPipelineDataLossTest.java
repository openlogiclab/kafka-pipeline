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
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression tests for paths that used to skip records or commit unprocessed ones. */
class KafkaPipelineDataLossTest {

  private static final String TOPIC = "data-loss";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

  private MockConsumer<String, String> mockConsumer;
  private KafkaPipeline<String, String> pipeline;
  private final CountDownLatch release = new CountDownLatch(1);

  @BeforeEach
  void setUp() {
    mockConsumer = new MockConsumer<>("earliest");
  }

  @AfterEach
  void tearDown() throws Exception {
    release.countDown();
    if (pipeline != null && pipeline.isRunning()) {
      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  private PipelineConfig.Builder<String, String> baseBuilder(int maxPollRecords) {
    Properties p = new Properties();
    p.put("bootstrap.servers", "dummy:9092");
    p.put("group.id", "test-group");
    p.put("max.poll.records", String.valueOf(maxPollRecords));
    mockConsumer.setMaxPollRecords(maxPollRecords);
    return PipelineConfig.<String, String>builder()
        .consumerProperties(p)
        .topics(TOPIC)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(20))
        .shutdownTimeout(Duration.ofSeconds(5));
  }

  private void start(TopicPartition... tps) throws InterruptedException {
    pipeline.startAsync();
    Thread.sleep(100);
    Map<TopicPartition, Long> beginning = new java.util.HashMap<>();
    for (TopicPartition tp : tps) beginning.put(tp, 0L);
    mockConsumer.updateBeginningOffsets(beginning);
    mockConsumer.rebalance(List.of(tps));
  }

  private void addRecords(TopicPartition tp, long from, long to) {
    for (long i = from; i < to; i++) {
      mockConsumer.addRecord(
          new ConsumerRecord<>(tp.topic(), tp.partition(), i, "k" + i, tp.partition() + ":" + i));
    }
  }

  /**
   * Simulates a broker log: before each poll, serves up to {@code batch} records starting at the
   * consumer's current position (unless the partition is paused). Unlike {@link
   * MockConsumer#addRecord}, records are served again after a seek back.
   */
  private void serveLog(TopicPartition tp, long end, int batch) {
    mockConsumer.schedulePollTask(
        () -> {
          if (mockConsumer.assignment().contains(tp) && !mockConsumer.paused().contains(tp)) {
            long pos = mockConsumer.position(tp);
            addRecords(tp, pos, Math.min(pos + batch, end));
          }
          if (pipeline.isRunning()) serveLog(tp, end, batch);
        });
  }

  private static boolean await(BooleanSupplier cond) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }

  private void blockOnFirstRecord(TopicPartition tp, ConsumerRecord<String, String> record) {
    if (record.partition() == tp.partition() && record.offset() == 0L) {
      try {
        release.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  void partitionAssignedWhileThrottled_recordsAreNotDropped() throws Exception {
    Map<String, AtomicInteger> processed = new ConcurrentHashMap<>();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .backpressure(
                BackpressureConfig.builder()
                    .highWatermark(5)
                    .lowWatermark(2)
                    .criticalThreshold(50)
                    .build())
            .handler(
                record -> {
                  blockOnFirstRecord(TP0, record);
                  processed
                      .computeIfAbsent(record.value(), v -> new AtomicInteger())
                      .incrementAndGet();
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);

    addRecords(TP0, 0, 10);
    assertTrue(await(() -> mockConsumer.paused().contains(TP0)), "TP0 should be throttled");

    // A rebalance hands TP1 to this consumer while it is throttled.
    mockConsumer.updateBeginningOffsets(Map.of(TP1, 0L));
    mockConsumer.rebalance(List.of(TP0, TP1));
    addRecords(TP1, 0, 10);
    Thread.sleep(300);

    release.countDown();
    // TP0's unpolled records were cleared by MockConsumer.rebalance; re-add from its position.
    long tp0Position = mockConsumer.position(TP0);
    addRecords(TP0, tp0Position, 10);

    assertTrue(
        await(() -> processed.keySet().stream().filter(v -> v.startsWith("1:")).count() == 10),
        "All TP1 records should be processed, got " + processed.keySet());
    processed.values().forEach(c -> assertEquals(1, c.get()));
  }

  @Test
  void fullWindow_seeksBackInsteadOfDroppingRecords() throws Exception {
    Map<Long, AtomicInteger> processed = new ConcurrentHashMap<>();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 2)
            .maxWindowSize(8)
            .handler(
                record -> {
                  blockOnFirstRecord(TP0, record);
                  processed
                      .computeIfAbsent(record.offset(), o -> new AtomicInteger())
                      .incrementAndGet();
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);

    // Offset 0 blocks; 1..7 complete behind it and fill the window (max 8). The second poll
    // returns 5..9, of which 8 and 9 do not fit.
    serveLog(TP0, 20, 5);
    assertTrue(await(() -> mockConsumer.paused().contains(TP0)), "TP0 should be window-paused");
    assertTrue(await(() -> processed.size() == 7), "1..7 should complete behind the stuck record");
    assertEquals(8L, mockConsumer.position(TP0), "consumer should be seeked back to offset 8");

    release.countDown();

    assertTrue(await(() -> processed.size() == 20), "Expected 0..19, got " + processed.keySet());
    processed.values().forEach(c -> assertEquals(1, c.get()));
    assertTrue(
        await(
            () -> {
              OffsetAndMetadata committed = mockConsumer.committed(Set.of(TP0)).get(TP0);
              return committed != null && committed.offset() == 20L;
            }),
        "Offsets through 19 should be committed");
  }

  @Test
  void shutdownInterruptDuringRetry_doesNotDlqOrCommitRecord() throws Exception {
    AtomicInteger dlqCalls = new AtomicInteger();
    CountDownLatch firstAttempt = new CountDownLatch(1);
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .shutdownTimeout(Duration.ofMillis(200))
            .errorStrategy(
                ErrorStrategy.<String, String>builder()
                    .maxRetries(3)
                    .retryBackoff(Duration.ofSeconds(30))
                    .dlqHandler((record, error) -> dlqCalls.incrementAndGet())
                    .build())
            .handler(
                record -> {
                  firstAttempt.countDown();
                  throw new RuntimeException("downstream unavailable");
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    addRecords(TP0, 0, 1);

    assertTrue(firstAttempt.await(5, TimeUnit.SECONDS));
    Thread.sleep(100); // now sleeping in retry backoff
    pipeline.stop();
    pipeline.awaitShutdown();

    assertEquals(0, dlqCalls.get(), "an interrupted record must not be sent to the DLQ");
    OffsetAndMetadata committed = mockConsumer.committed(Set.of(TP0)).get(TP0);
    assertTrue(
        committed == null || committed.offset() == 0L,
        "offset 0 must stay uncommitted so it is redelivered, got " + committed);
  }
}
