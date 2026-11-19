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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.backpressure.RecordBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** Pipeline-level behaviour of the backpressure settings and of shutdown. */
class BackpressureAndShutdownTest {

  private static final String TOPIC = "orders";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);

  private static PipelineConfig.Builder<String, String> builder() {
    Properties p = new Properties();
    p.put("bootstrap.servers", "dummy:9092");
    p.put("group.id", "bp-test");
    return PipelineConfig.<String, String>builder()
        .consumerProperties(p)
        .topic(TOPIC)
        .concurrency(ThreadMode.VIRTUAL, 4)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(20))
        .shutdownTimeout(Duration.ofSeconds(2));
  }

  private static boolean await(BooleanSupplier cond) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }

  private static void assign(MockConsumer<String, String> consumer) throws InterruptedException {
    assertTrue(await(() -> consumer.subscription().contains(TOPIC)));
    consumer.updateBeginningOffsets(Map.of(TP0, 0L));
    consumer.rebalance(List.of(TP0));
  }

  @Test
  void disabledRecordBackpressure_leavesOtherSensorsActive() throws Exception {
    AtomicBoolean overloaded = new AtomicBoolean();
    BackpressureSensor downstream =
        new BackpressureSensor() {
          @Override
          public BackpressureStatus evaluate() {
            return overloaded.get() ? BackpressureStatus.THROTTLE : BackpressureStatus.OK;
          }

          @Override
          public String name() {
            return "downstream";
          }

          @Override
          public String statusDetail() {
            return "downstream overloaded=" + overloaded.get();
          }
        };
    MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    KafkaPipeline<String, String> pipeline =
        new KafkaPipeline<>(
            builder()
                .handler(r -> {})
                .recordBackpressure(RecordBackpressureConfig.disabled())
                .addBackpressureSensor(downstream)
                .build(),
            consumer);
    try {
      pipeline.startAsync();
      assign(consumer);

      overloaded.set(true);
      assertTrue(
          await(() -> consumer.paused().contains(TP0)),
          "turning off record-count backpressure must not turn off the other sensors");
      assertEquals(BackpressureStatus.THROTTLE, pipeline.metrics().backpressureStatus());
      overloaded.set(false);
      assertTrue(await(() -> !consumer.paused().contains(TP0)));
    } finally {
      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  @Test
  void errorFromASinkClose_stillClosesTheConsumerAndReleasesAwaitShutdown() throws Exception {
    MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    RecordSink<String> sink =
        new RecordSink<>() {
          @Override
          public CompletionStage<Void> write(SinkContext ctx, List<String> outputs) {
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public void close() {
            throw new AssertionError("broken close()");
          }
        };
    KafkaPipeline<String, String> pipeline =
        new KafkaPipeline<>(
            builder().processor(r -> List.of(r.value())).sink(sink).build(), consumer, true);
    pipeline.startAsync();
    assign(consumer);
    consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k", "v"));

    pipeline.stop();
    CountDownLatch shutDown = new CountDownLatch(1);
    Thread.startVirtualThread(
        () -> {
          try {
            pipeline.awaitShutdown();
            shutDown.countDown();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
    assertTrue(shutDown.await(10, TimeUnit.SECONDS), "awaitShutdown() must return");
    assertTrue(consumer.closed(), "the consumer is closed even though close() threw an Error");
  }
}
