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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Ordering modes, end to end through the pipeline. */
class OrderingTest {

  private static final String TOPIC = "ordered";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
  private static final int PER_PARTITION = 300;

  private MockConsumer<String, String> mockConsumer;
  private KafkaPipeline<String, String> pipeline;

  // Observed processing order and overlap, per key and per partition.
  private final Map<String, List<Long>> orderByKey = new ConcurrentHashMap<>();
  private final Map<Integer, List<Long>> orderByPartition = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> activeByKey = new ConcurrentHashMap<>();
  private final Map<Integer, AtomicInteger> activeByPartition = new ConcurrentHashMap<>();
  private final AtomicInteger maxActivePerKey = new AtomicInteger();
  private final AtomicInteger maxActivePerPartition = new AtomicInteger();
  private final AtomicInteger maxActiveOverall = new AtomicInteger();
  private final AtomicInteger activeOverall = new AtomicInteger();
  private final AtomicInteger processed = new AtomicInteger();

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

  @Test
  void keyOrdering_sameKeyRunsOneAtATimeInOffsetOrder_keysRunInParallel() throws Exception {
    run(ProcessingOrder.KEY, false);

    orderByKey.forEach((key, offsets) -> assertSorted(offsets, "key " + key));
    assertEquals(1, maxActivePerKey.get(), "two records of one key ran at the same time");
    assertTrue(maxActiveOverall.get() > 1, "different keys should run in parallel");
  }

  @Test
  void partitionOrdering_oneRecordPerPartitionAtATime() throws Exception {
    run(ProcessingOrder.PARTITION, false);

    orderByPartition.forEach((p, offsets) -> assertSorted(offsets, "partition " + p));
    assertEquals(1, maxActivePerPartition.get());
    assertTrue(maxActiveOverall.get() > 1, "the two partitions should run in parallel");
  }

  @Test
  void unordered_runsRecordsOfAKeyInParallel() throws Exception {
    run(ProcessingOrder.UNORDERED, false);
    assertTrue(maxActivePerKey.get() > 1, "unordered mode should not serialize keys");
  }

  @Test
  void batchMode_orderedKeepsOneBatchPerPartitionInFlight() throws Exception {
    run(ProcessingOrder.PARTITION, true);

    orderByPartition.forEach((p, offsets) -> assertSorted(offsets, "partition " + p));
    assertEquals(1, maxActivePerPartition.get(), "two batches of one partition overlapped");
  }

  private void run(ProcessingOrder order, boolean batch) throws Exception {
    Properties p = new Properties();
    p.put("bootstrap.servers", "dummy:9092");
    p.put("group.id", "test-group");
    p.put("max.poll.records", "50");
    mockConsumer.setMaxPollRecords(50);
    PipelineConfig.Builder<String, String> builder =
        PipelineConfig.<String, String>builder()
            .consumerProperties(p)
            .topic(TOPIC)
            .concurrency(ThreadMode.PLATFORM, 8)
            .ordering(order)
            .commitInterval(Duration.ofMillis(50))
            .pollTimeout(Duration.ofMillis(10));
    if (batch) {
      builder.batchHandler(
          (tp, records) -> {
            enter(null, tp.partition());
            try {
              for (ConsumerRecord<String, String> r : records) record(r);
              Thread.sleep(ThreadLocalRandom.current().nextInt(0, 3));
            } finally {
              exit(null, tp.partition());
            }
          });
    } else {
      builder.handler(
          r -> {
            enter(r.key(), r.partition());
            try {
              Thread.sleep(ThreadLocalRandom.current().nextInt(0, 3));
              record(r);
            } finally {
              exit(r.key(), r.partition());
            }
          });
    }
    pipeline = new KafkaPipeline<>(builder.build(), mockConsumer);
    pipeline.startAsync();
    Thread.sleep(100);
    Map<TopicPartition, Long> beginning = new HashMap<>();
    beginning.put(TP0, 0L);
    beginning.put(TP1, 0L);
    mockConsumer.updateBeginningOffsets(beginning);
    mockConsumer.rebalance(List.of(TP0, TP1));
    serveLog();

    await(() -> processed.get() >= 2 * PER_PARTITION, () -> "processed " + processed.get());
  }

  /**
   * Serves records 0..PER_PARTITION-1 on both partitions from the current position (refetch-safe).
   */
  private void serveLog() {
    mockConsumer.schedulePollTask(
        () -> {
          for (TopicPartition tp : List.of(TP0, TP1)) {
            if (!mockConsumer.paused().contains(tp)) {
              long pos = mockConsumer.position(tp);
              for (long o = pos; o < Math.min(pos + 50, PER_PARTITION); o++) {
                mockConsumer.addRecord(
                    new ConsumerRecord<>(TOPIC, tp.partition(), o, "k" + (o % 5), "v" + o));
              }
            }
          }
          if (pipeline.isRunning()) serveLog();
        });
  }

  private void enter(String key, int partition) {
    maxActiveOverall.accumulateAndGet(activeOverall.incrementAndGet(), Math::max);
    maxActivePerPartition.accumulateAndGet(
        activeByPartition.computeIfAbsent(partition, k -> new AtomicInteger()).incrementAndGet(),
        Math::max);
    if (key != null) {
      maxActivePerKey.accumulateAndGet(
          activeByKey
              .computeIfAbsent(partition + "/" + key, k -> new AtomicInteger())
              .incrementAndGet(),
          Math::max);
    }
  }

  private void exit(String key, int partition) {
    activeOverall.decrementAndGet();
    activeByPartition.get(partition).decrementAndGet();
    if (key != null) activeByKey.get(partition + "/" + key).decrementAndGet();
  }

  private void record(ConsumerRecord<String, String> r) {
    orderByKey
        .computeIfAbsent(
            r.partition() + "/" + r.key(), k -> Collections.synchronizedList(new ArrayList<>()))
        .add(r.offset());
    orderByPartition
        .computeIfAbsent(r.partition(), k -> Collections.synchronizedList(new ArrayList<>()))
        .add(r.offset());
    processed.incrementAndGet();
  }

  private static void assertSorted(List<Long> offsets, String what) {
    List<Long> copy;
    synchronized (offsets) {
      copy = new ArrayList<>(offsets);
    }
    for (int i = 1; i < copy.size(); i++) {
      assertTrue(copy.get(i - 1) < copy.get(i), what + " out of order at " + i + ": " + copy);
    }
  }

  private static void await(BooleanSupplier cond, java.util.function.Supplier<String> msg)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + 20_000;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) fail(msg.get());
      Thread.sleep(10);
    }
  }
}
