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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** The pipeline fails fast and reports why, instead of looking healthy while it cannot work. */
class KafkaPipelineFailFastTest {

  private static final String TOPIC = "fail-fast";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

  private KafkaPipeline<String, String> pipeline;

  @AfterEach
  void tearDown() throws Exception {
    if (pipeline != null) {
      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  private static PipelineConfig.Builder<String, String> builder() {
    Properties p = new Properties();
    p.put("bootstrap.servers", "dummy:9092");
    p.put("group.id", "test-group");
    return PipelineConfig.<String, String>builder()
        .consumerProperties(p)
        .topics(TOPIC)
        .concurrency(ThreadMode.PLATFORM, 2)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(20))
        .shutdownTimeout(Duration.ofSeconds(2));
  }

  @Nested
  class Startup {

    @Test
    void unreachableBroker_startThrowsAndPipelineIsNotRunning() {
      MockConsumer<String, String> consumer =
          new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<String, List<PartitionInfo>> listTopics() {
              throw new TimeoutException("Timeout expired while fetching topic metadata");
            }
          };
      pipeline = new KafkaPipeline<>(builder().handler(r -> {}).build(), consumer);

      TimeoutException e = assertThrows(TimeoutException.class, pipeline::start);
      assertEquals(e, pipeline.failure().orElseThrow());
      assertFalse(pipeline.isRunning());
      assertThrows(IllegalStateException.class, pipeline::start, "a pipeline starts only once");
    }
  }

  @Nested
  class StopDuringStartup {

    @Test
    void stopWhileCheckingBrokers_isACleanStop() throws Exception {
      java.util.concurrent.CountDownLatch checking = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.atomic.AtomicReference<KafkaPipeline<String, String>> ref =
          new java.util.concurrent.atomic.AtomicReference<>();
      MockConsumer<String, String> consumer =
          new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<String, List<PartitionInfo>> listTopics() {
              checking.countDown();
              while (ref.get().isRunning()) Thread.onSpinWait(); // blocked until stop()
              throw new org.apache.kafka.common.errors.WakeupException();
            }
          };
      pipeline = new KafkaPipeline<>(builder().handler(r -> {}).build(), consumer);
      ref.set(pipeline);
      Thread t = pipeline.startAsync();
      assertTrue(checking.await(5, TimeUnit.SECONDS));
      pipeline.stop();
      t.join(5000);
      assertFalse(t.isAlive());
      assertTrue(pipeline.failure().isEmpty(), "stop() during startup is not a failure");
    }
  }

  @Nested
  class FatalRuntimeErrors {

    @Test
    void authorizationError_stopsPipelineInsteadOfRetryingForever() throws Exception {
      MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
      pipeline = new KafkaPipeline<>(builder().handler(r -> {}).build(), consumer);
      Thread t = pipeline.startAsync();
      assertTrue(await(pipeline::isRunning));

      consumer.setPollException(new GroupAuthorizationException("not authorized"));

      t.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(t.isAlive());
      assertFalse(pipeline.isRunning());
      assertInstanceOf(GroupAuthorizationException.class, pipeline.failure().orElseThrow());
    }

    @Test
    void cleanStop_reportsNoFailure() throws Exception {
      MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
      pipeline = new KafkaPipeline<>(builder().handler(r -> {}).build(), consumer);
      pipeline.startAsync();
      assertTrue(await(pipeline::isRunning));
      pipeline.stop();
      pipeline.awaitShutdown();
      assertFalse(pipeline.isRunning());
      assertTrue(pipeline.failure().isEmpty());
    }
  }

  @Nested
  class PartitionInitFailure {

    @Test
    void positionFailure_keepsPartitionPausedAndRetries_othersFlow() throws Exception {
      AtomicInteger tp1Failures = new AtomicInteger();
      MockConsumer<String, String> consumer =
          new MockConsumer<>("earliest") {
            @Override
            public synchronized long position(TopicPartition partition) {
              if (partition.equals(TP1) && tp1Failures.getAndIncrement() == 0) {
                throw new TimeoutException("position timed out");
              }
              return super.position(partition);
            }

            @Override
            public synchronized long position(TopicPartition partition, Duration timeout) {
              return position(partition);
            }
          };
      List<String> seen = new CopyOnWriteArrayList<>();
      pipeline = new KafkaPipeline<>(builder().handler(r -> seen.add(r.value())).build(), consumer);
      pipeline.startAsync();
      assertTrue(await(pipeline::isRunning));

      consumer.updateBeginningOffsets(Map.of(TP0, 0L, TP1, 0L));
      consumer.rebalance(List.of(TP0, TP1));
      consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "tp0-a"));
      consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0L, "k", "tp1-a"));

      // TP0 flows right away; TP1 starts once its initialization is retried (about 1 s later).
      assertTrue(await(() -> seen.contains("tp0-a")));
      assertTrue(await(() -> seen.contains("tp1-a")), "TP1 must be processed after its retry");
      assertTrue(tp1Failures.get() >= 2);
      assertTrue(pipeline.isRunning());
    }
  }

  private static boolean await(java.util.function.BooleanSupplier cond)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }
}
