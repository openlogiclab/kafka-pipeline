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

import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import io.github.openlogiclab.kafkapipeline.metrics.HandlerAttempt;
import io.github.openlogiclab.kafkapipeline.metrics.MetricsBinding;
import io.github.openlogiclab.kafkapipeline.metrics.MetricsExporter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The exporter SPI as a user sees it: bind on start, pull and push while running, close at end. */
class MetricsExporterTest {

  private static final String TOPIC = "orders";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);

  private MockConsumer<String, String> consumer;
  private KafkaPipeline<String, String> pipeline;

  /** Records everything the pipeline gives it. */
  static final class RecordingExporter implements MetricsExporter {
    final List<String> events = new CopyOnWriteArrayList<>();
    final List<HandlerAttempt> attempts = new CopyOnWriteArrayList<>();
    volatile MetricsBinding binding;

    @Override
    public void bind(MetricsBinding binding) {
      this.binding = binding;
      events.add("bind:" + binding.pipelineName());
      binding.onHandlerAttempt(attempts::add);
    }

    @Override
    public void close() {
      events.add("close");
    }
  }

  @BeforeEach
  void setUp() {
    consumer = new MockConsumer<>("earliest");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (pipeline != null && pipeline.isRunning()) {
      pipeline.stop();
      pipeline.awaitShutdown();
    }
  }

  private PipelineConfig.Builder<String, String> builder() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "dummy:9092");
    props.put("group.id", "orders-consumer");
    return PipelineConfig.<String, String>builder()
        .consumerProperties(props)
        .topic(TOPIC)
        .concurrency(ThreadMode.PLATFORM, 2)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(20))
        .shutdownTimeout(Duration.ofSeconds(5));
  }

  private void startWithRecords(int count) throws InterruptedException {
    pipeline.startAsync();
    assertTrue(await(() -> consumer.subscription().contains(TOPIC)));
    consumer.updateBeginningOffsets(Map.of(TP0, 0L));
    consumer.rebalance(List.of(TP0));
    for (int i = 0; i < count; i++) {
      consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
    }
  }

  private static boolean await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) return false;
      Thread.sleep(10);
    }
    return true;
  }

  @Test
  void exporterIsBoundOnStart_seesPullAndPush_andIsClosedAfterShutdown() throws Exception {
    RecordingExporter exporter = new RecordingExporter();
    pipeline =
        new KafkaPipeline<>(builder().handler(r -> {}).metricsExporter(exporter).build(), consumer);
    assertTrue(exporter.events.isEmpty(), "not bound before start");

    startWithRecords(3);
    assertTrue(await(() -> exporter.attempts.size() == 3));

    assertEquals("orders-consumer", exporter.binding.pipelineName());
    assertEquals(3, exporter.binding.snapshot().recordsProcessed());
    assertEquals(3, exporter.binding.snapshot().handlerDuration().count());
    for (HandlerAttempt attempt : exporter.attempts) {
      assertEquals(TP0, attempt.partition());
      assertEquals(AttemptOutcome.SUCCESS, attempt.outcome());
      assertTrue(attempt.durationNanos() >= 0);
    }

    pipeline.stop();
    pipeline.awaitShutdown();
    assertEquals(List.of("bind:orders-consumer", "close"), exporter.events);
  }

  @Test
  void failingListenerNeverAffectsProcessing() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    MetricsExporter broken =
        binding ->
            binding.onHandlerAttempt(
                attempt -> {
                  calls.incrementAndGet();
                  throw new IllegalStateException("broken exporter");
                });
    RecordingExporter healthy = new RecordingExporter();
    pipeline =
        new KafkaPipeline<>(
            builder().handler(r -> {}).metricsExporter(broken).metricsExporter(healthy).build(),
            consumer);

    startWithRecords(5);

    assertTrue(await(() -> pipeline.metrics().recordsProcessed() == 5));
    assertEquals(0, pipeline.metrics().recordsFailed());
    assertEquals(5, calls.get());
    assertTrue(await(() -> healthy.attempts.size() == 5), "later exporters still get events");
    assertTrue(pipeline.isRunning());
  }

  @Test
  void failingBindFailsStart_andClosesExportersAlreadyBound() throws Exception {
    RecordingExporter first = new RecordingExporter();
    MetricsExporter misconfigured =
        binding -> {
          throw new IllegalArgumentException("bad exporter config");
        };
    pipeline =
        new KafkaPipeline<>(
            builder()
                .handler(r -> {})
                .metricsExporter(first)
                .metricsExporter(misconfigured)
                .build(),
            consumer);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, pipeline::start);

    assertEquals("bad exporter config", e.getMessage());
    assertSame(e, pipeline.failure().orElseThrow());
    assertEquals(List.of("bind:orders-consumer", "close"), first.events);
  }
}
