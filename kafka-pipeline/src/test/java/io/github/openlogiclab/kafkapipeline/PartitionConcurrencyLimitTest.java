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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A slow partition must not take every worker when maxConcurrencyPerPartition is set. */
class PartitionConcurrencyLimitTest {

  private static final String TOPIC = "noisy";
  private static final TopicPartition SLOW = new TopicPartition(TOPIC, 0);
  private static final TopicPartition FAST = new TopicPartition(TOPIC, 1);

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

  @Test
  void slowPartitionHoldsAtMostItsLimit_otherPartitionsKeepFlowing() throws Exception {
    AtomicInteger slowActive = new AtomicInteger();
    AtomicInteger slowMaxActive = new AtomicInteger();
    AtomicInteger slowDone = new AtomicInteger();
    AtomicInteger fastDone = new AtomicInteger();

    pipeline =
        new KafkaPipeline<>(
            builder()
                .concurrency(ThreadMode.PLATFORM, 4)
                .maxConcurrencyPerPartition(1)
                .handler(
                    record -> {
                      if (record.partition() == SLOW.partition()) {
                        slowMaxActive.accumulateAndGet(slowActive.incrementAndGet(), Math::max);
                        try {
                          release.await(10, TimeUnit.SECONDS);
                        } finally {
                          slowActive.decrementAndGet();
                        }
                        slowDone.incrementAndGet();
                      } else {
                        fastDone.incrementAndGet();
                      }
                    })
                .build(),
            mockConsumer);
    start(SLOW, FAST);

    // Enough slow records to occupy all 4 workers if nothing limited them. With 2 partitions
    // assigned, the enforced limit is max(1, ceil(4 / 2)) = 2.
    addRecords(SLOW, 10);
    await(() -> slowActive.get() == 2, "workers should pick up the slow partition");
    Thread.sleep(200); // give idle workers the chance to (wrongly) take more slow records

    addRecords(FAST, 20);
    await(() -> fastDone.get() == 20, "fast partition starved: " + fastDone.get() + "/20");
    assertEquals(2, slowMaxActive.get(), "slow partition exceeded its limit");

    release.countDown();
    await(() -> slowDone.get() == 10, "slow partition should finish once released");
    assertEquals(2, slowMaxActive.get());
  }

  @Test
  void singleAssignedPartition_canUseTheWholePool() throws Exception {
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maxActive = new AtomicInteger();
    AtomicInteger done = new AtomicInteger();

    pipeline =
        new KafkaPipeline<>(
            builder()
                .concurrency(ThreadMode.PLATFORM, 8)
                .maxConcurrencyPerPartition(2)
                .handler(
                    record -> {
                      maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                      try {
                        release.await(10, TimeUnit.SECONDS);
                      } finally {
                        active.decrementAndGet();
                      }
                      done.incrementAndGet();
                    })
                .build(),
            mockConsumer);
    start(SLOW);

    // Nothing to protect, so the fair-share floor lifts the limit of 2 to all 8 workers.
    addRecords(SLOW, 12);
    await(() -> active.get() == 8, "expected all 8 workers busy, got " + active.get());
    Thread.sleep(200);
    assertEquals(8, maxActive.get());

    release.countDown();
    await(() -> done.get() == 12, "all records should finish, got " + done.get());
  }

  private PipelineConfig.Builder<String, String> builder() {
    Properties p = new Properties();
    p.put("bootstrap.servers", "dummy:9092");
    p.put("group.id", "test-group");
    p.put("max.poll.records", "50");
    return PipelineConfig.<String, String>builder()
        .consumerProperties(p)
        .topic(TOPIC)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(20))
        .shutdownTimeout(Duration.ofSeconds(5));
  }

  private void start(TopicPartition... tps) throws InterruptedException {
    pipeline.startAsync();
    Thread.sleep(100);
    Map<TopicPartition, Long> beginning = new HashMap<>();
    for (TopicPartition tp : tps) beginning.put(tp, 0L);
    mockConsumer.updateBeginningOffsets(beginning);
    mockConsumer.rebalance(List.of(tps));
  }

  private void addRecords(TopicPartition tp, int count) {
    mockConsumer.schedulePollTask(
        () -> {
          for (int i = 0; i < count; i++) {
            mockConsumer.addRecord(
                new ConsumerRecord<>(tp.topic(), tp.partition(), i, "k" + i, "v" + i));
          }
        });
  }

  private static void await(BooleanSupplier cond, String message) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) fail(message);
      Thread.sleep(10);
    }
  }
}
