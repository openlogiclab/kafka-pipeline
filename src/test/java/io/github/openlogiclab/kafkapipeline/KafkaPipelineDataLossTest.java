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

  private void await(BooleanSupplier cond, String message) throws InterruptedException {
    assertTrue(await(cond), message);
  }

  private boolean committedAt(TopicPartition tp, long offset) {
    OffsetAndMetadata committed = mockConsumer.committed(Set.of(tp)).get(tp);
    return committed != null && committed.offset() == offset;
  }

  @Test
  void batchMode_fullWindow_seeksBackToBatchStart() throws Exception {
    Map<Long, AtomicInteger> processed = new ConcurrentHashMap<>();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 2)
            .maxWindowSize(8)
            .batchHandler(
                (tp, batch) -> {
                  if (batch.get(0).offset() == 0L) {
                    release.await(10, TimeUnit.SECONDS);
                  }
                  for (ConsumerRecord<String, String> r : batch) {
                    processed
                        .computeIfAbsent(r.offset(), o -> new AtomicInteger())
                        .incrementAndGet();
                  }
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);

    // Batch 0..4 blocks and stays in the window (5 of 8). Batch 5..9 cannot fit as a whole, so
    // none of it is registered and the consumer seeks back to its first offset.
    serveLog(TP0, 20, 5);
    await(() -> mockConsumer.paused().contains(TP0), "TP0 should be window-paused");
    assertEquals(5L, mockConsumer.position(TP0), "consumer should be seeked back to offset 5");
    assertTrue(processed.isEmpty());

    release.countDown();

    await(() -> processed.size() == 20, "Expected 0..19, got " + processed.keySet());
    processed.values().forEach(c -> assertEquals(1, c.get()));
    await(() -> committedAt(TP0, 20L), "Offsets through 19 should be committed");
  }

  @Test
  void batchMode_shutdownInterruptDuringRetry_doesNotDlqOrCommitBatch() throws Exception {
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
            .batchHandler(
                (tp, batch) -> {
                  firstAttempt.countDown();
                  throw new RuntimeException("downstream unavailable");
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    addRecords(TP0, 0, 3);

    assertTrue(firstAttempt.await(5, TimeUnit.SECONDS));
    Thread.sleep(100); // now sleeping in retry backoff
    pipeline.stop();
    pipeline.awaitShutdown();

    assertEquals(0, dlqCalls.get(), "an interrupted batch must not be sent to the DLQ");
    OffsetAndMetadata committed = mockConsumer.committed(Set.of(TP0)).get(TP0);
    assertTrue(
        committed == null || committed.offset() == 0L,
        "the batch must stay uncommitted so it is redelivered, got " + committed);
  }

  @Test
  void stuckPartitionPauses_otherPartitionKeepsFlowing() throws Exception {
    Map<String, AtomicInteger> processed = new ConcurrentHashMap<>();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 3)
            .maxWindowSize(8)
            .handler(
                record -> {
                  blockOnFirstRecord(TP0, record);
                  processed
                      .computeIfAbsent(record.value(), v -> new AtomicInteger())
                      .incrementAndGet();
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0, TP1);

    serveLog(TP0, 20, 5);
    serveLog(TP1, 50, 5);

    await(() -> mockConsumer.paused().contains(TP0), "TP0 should be window-paused");
    await(
        () -> processed.keySet().stream().filter(v -> v.startsWith("1:")).count() == 50,
        "TP1 should finish all 50 records while TP0 is stuck, got " + processed.keySet());
    assertFalse(mockConsumer.paused().contains(TP1), "TP1 must not be paused");
    assertFalse(processed.containsKey("0:0"), "TP0's stuck record is still running");
    await(() -> committedAt(TP1, 50L), "TP1 should commit while TP0 is stuck");

    release.countDown();

    await(
        () -> processed.keySet().stream().filter(v -> v.startsWith("0:")).count() == 20,
        "TP0 should finish after the stuck record completes, got " + processed.keySet());
    processed.values().forEach(c -> assertEquals(1, c.get()));
    await(() -> committedAt(TP0, 20L), "TP0 should commit through 19");
  }

  @Test
  void windowPausedPartitionRevokedAndReassigned_isResumedAndReprocessed() throws Exception {
    Map<Long, AtomicInteger> processed = new ConcurrentHashMap<>();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 2)
            .maxWindowSize(8)
            .drainTimeout(Duration.ofMillis(200))
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

    serveLog(TP0, 20, 5);
    await(() -> mockConsumer.paused().contains(TP0), "TP0 should be window-paused");

    // Revoke TP0 while offset 0 is still stuck. Nothing past it may be committed.
    mockConsumer.rebalance(List.of());
    Thread.sleep(300);
    OffsetAndMetadata committed = mockConsumer.committed(Set.of(TP0)).get(TP0);
    assertTrue(
        committed == null || committed.offset() == 0L,
        "nothing past the stuck record may be committed on revoke, got " + committed);
    release.countDown();

    // Reassigned: the partition starts unpaused from its committed offset and every record is
    // processed again (at-least-once).
    processed.clear();
    mockConsumer.rebalance(List.of(TP0));
    await(() -> processed.keySet().containsAll(rangeSet(0, 20)), "Expected 0..19 after reassign");
    assertFalse(mockConsumer.paused().contains(TP0));
    await(() -> committedAt(TP0, 20L), "TP0 should commit through 19 after reassign");
  }

  private static Set<Long> rangeSet(long from, long to) {
    Set<Long> set = new java.util.HashSet<>();
    for (long i = from; i < to; i++) set.add(i);
    return set;
  }

  @Test
  void stopWhileOutsidePoll_finalCommitStillLands() throws Exception {
    // Real KafkaConsumer semantics: wakeup() makes the *next* blocking call throw, which can be
    // the shutdown commitSync if stop() lands while the poll thread is not inside poll().
    AtomicInteger wakeupThrows = new AtomicInteger();
    mockConsumer =
        new MockConsumer<>("earliest") {
          private volatile boolean wakeupPending;

          @Override
          public void wakeup() {
            wakeupPending = true; // not consumed by poll(), as if stop() landed outside it
          }

          @Override
          public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (wakeupPending) {
              wakeupPending = false;
              wakeupThrows.incrementAndGet();
              throw new org.apache.kafka.common.errors.WakeupException();
            }
            super.commitSync(offsets);
          }
        };
    AtomicInteger processed = new AtomicInteger();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .commitInterval(Duration.ofMinutes(10)) // only the final commit can commit
            .handler(record -> processed.incrementAndGet())
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    addRecords(TP0, 0, 3);
    await(() -> processed.get() == 3, "records should be processed");

    pipeline.stop();
    pipeline.awaitShutdown();

    assertEquals(1, wakeupThrows.get(), "the pending wakeup should have fired on the commit");
    assertTrue(committedAt(TP0, 3L), "the final commit must still land after the wakeup");
  }

  @Test
  void recordFinishingAfterRevoke_releasesInFlightCount() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .drainTimeout(Duration.ofMillis(100))
            .handler(
                record -> {
                  started.countDown();
                  blockOnFirstRecord(TP0, record);
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    addRecords(TP0, 0, 1);
    assertTrue(started.await(5, TimeUnit.SECONDS));

    // Revoke while offset 0 is still running; the drain times out and the window is cleared.
    mockConsumer.rebalance(List.of());
    assertEquals(1, pipeline.metrics().inFlightRecords());

    // The record finishes after its partition is gone. Its ack fails, but the in-flight slot
    // must still be released or backpressure would never drain.
    release.countDown();
    await(
        () -> pipeline.metrics().inFlightRecords() == 0,
        "in-flight count leaked: " + pipeline.metrics().inFlightRecords());
    assertEquals(0, pipeline.metrics().inFlightBytes());
  }

  @Test
  void batchFinishingAfterRevoke_releasesInFlightCount() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .drainTimeout(Duration.ofMillis(100))
            .batchHandler(
                (tp, batch) -> {
                  started.countDown();
                  release.await(10, TimeUnit.SECONDS);
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    // Add all three inside one poll, so the batch can't start with only some of them.
    mockConsumer.schedulePollTask(() -> addRecords(TP0, 0, 3));
    assertTrue(started.await(5, TimeUnit.SECONDS));

    mockConsumer.rebalance(List.of());
    assertEquals(3, pipeline.metrics().inFlightRecords());

    release.countDown();
    await(
        () -> pipeline.metrics().inFlightRecords() == 0,
        "in-flight count leaked: " + pipeline.metrics().inFlightRecords());
  }

  @Test
  void recordFailingAfterRevoke_releasesInFlightCount() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .drainTimeout(Duration.ofMillis(100))
            .handler(
                record -> {
                  started.countDown();
                  release.await(10, TimeUnit.SECONDS);
                  throw new RuntimeException("fails after its partition was revoked");
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    addRecords(TP0, 0, 1);
    assertTrue(started.await(5, TimeUnit.SECONDS));

    mockConsumer.rebalance(List.of());
    release.countDown();

    await(
        () -> pipeline.metrics().inFlightRecords() == 0,
        "in-flight count leaked: " + pipeline.metrics().inFlightRecords());
  }

  @Test
  void batchFailingAfterRevoke_releasesInFlightCount() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .drainTimeout(Duration.ofMillis(100))
            .batchHandler(
                (tp, batch) -> {
                  started.countDown();
                  release.await(10, TimeUnit.SECONDS);
                  throw new RuntimeException("fails after its partition was revoked");
                })
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    addRecords(TP0, 0, 3);
    assertTrue(started.await(5, TimeUnit.SECONDS));

    mockConsumer.rebalance(List.of());
    release.countDown();

    await(
        () -> pipeline.metrics().inFlightRecords() == 0,
        "in-flight count leaked: " + pipeline.metrics().inFlightRecords());
  }

  @Test
  void fullDispatchQueue_pausesPartitionInsteadOfBlockingPollThread() throws Exception {
    Map<Long, AtomicInteger> processed = new ConcurrentHashMap<>();
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .dispatchQueueCapacity(5)
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

    // The single worker is stuck on offset 0, so the queue (5) fills on the second poll and the
    // rest of that poll is sent back with a seek. Whether the worker took offset 0 off the queue
    // before that poll is a race, so the seek lands on 5 (queue held 0..4) or 6 (queue held 1..5).
    serveLog(TP0, 30, 5);
    await(
        () -> mockConsumer.paused().contains(TP0), "TP0 should be paused while its queue is full");
    long position = mockConsumer.position(TP0);
    assertTrue(
        position == 5L || position == 6L,
        "rest of the poll should be fetched again later, position=" + position);

    release.countDown();

    await(() -> processed.size() == 30, "Expected 0..29, got " + processed.keySet());
    processed.values().forEach(c -> assertEquals(1, c.get()));
    await(() -> committedAt(TP0, 30L), "Offsets through 29 should be committed");
  }

  @Test
  void stopWithFullQueueAndHungHandler_shutsDownWithinShutdownTimeout() throws Exception {
    CountDownLatch neverReleased = new CountDownLatch(1);
    PipelineConfig<String, String> config =
        baseBuilder(5)
            .concurrency(ThreadMode.PLATFORM, 1)
            .dispatchQueueCapacity(2)
            .shutdownTimeout(Duration.ofMillis(500))
            .handler(record -> neverReleased.await(60, TimeUnit.SECONDS))
            .build();
    pipeline = new KafkaPipeline<>(config, mockConsumer);
    start(TP0);
    serveLog(TP0, 20, 5);
    await(() -> mockConsumer.paused().contains(TP0), "queue should fill while the handler hangs");

    long start = System.nanoTime();
    pipeline.stop();
    Thread waiter =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    pipeline.awaitShutdown();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    waiter.join(10_000);

    assertFalse(waiter.isAlive(), "shutdown hung behind a blocked poll thread");
    assertTrue(
        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5),
        "shutdown should finish shortly after shutdownTimeout");
  }
}
