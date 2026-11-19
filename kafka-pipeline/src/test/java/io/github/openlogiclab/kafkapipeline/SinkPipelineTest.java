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

import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.sink.RecordProcessor;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** End to end with a mock consumer and a mock producer: process, write, commit, close. */
class SinkPipelineTest {

  private static final String TOPIC = "orders";
  private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);

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
    p.put("group.id", "sink-test");
    return PipelineConfig.<String, String>builder()
        .consumerProperties(p)
        .topic(TOPIC)
        .concurrency(ThreadMode.VIRTUAL, 8)
        .commitInterval(Duration.ofMillis(50))
        .pollTimeout(Duration.ofMillis(20))
        .shutdownTimeout(Duration.ofSeconds(3));
  }

  private static MockProducer<byte[], byte[]> producer() {
    return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private static boolean await(BooleanSupplier cond) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }

  private static long committed(MockConsumer<String, String> consumer) {
    Map<TopicPartition, OffsetAndMetadata> c = consumer.committed(Set.of(TP0));
    OffsetAndMetadata m = c.get(TP0);
    return m == null ? -1 : m.offset();
  }

  private static void assign(MockConsumer<String, String> consumer) throws InterruptedException {
    // isRunning() turns true before the pipeline subscribes; wait for the subscription.
    assertTrue(await(() -> consumer.subscription().contains(TOPIC)));
    consumer.updateBeginningOffsets(Map.of(TP0, 0L));
    consumer.rebalance(List.of(TP0));
  }

  @Nested
  class EndToEnd {

    @Test
    void keyOrdering_outputsOfAKeyAreWrittenInInputOrder() throws Exception {
      MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
      MockProducer<byte[], byte[]> producer = producer();
      // Slow, uneven writes: without ordering, later records of a key could overtake earlier ones.
      RecordSink<String> slowKafka =
          (ctx, output) ->
              java.util.concurrent.CompletableFuture.runAsync(
                      () -> {
                        try {
                          Thread.sleep(
                              java.util.concurrent.ThreadLocalRandom.current().nextInt(10));
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      })
                  .thenRun(
                      () ->
                          producer.send(
                              new ProducerRecord<>(
                                  "out", output.getBytes(StandardCharsets.UTF_8))));
      pipeline =
          new KafkaPipeline<>(
              builder()
                  .ordering(ProcessingOrder.KEY)
                  .processor(r -> Optional.of(r.key() + ":" + r.offset()))
                  .sink(slowKafka)
                  .build(),
              consumer);
      pipeline.startAsync();
      assign(consumer);
      int perKey = 20;
      for (int i = 0; i < perKey * 3; i++) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "key" + (i % 3), "v"));
      }

      assertTrue(await(() -> committed(consumer) == perKey * 3));
      Map<String, List<Long>> byKey = new java.util.TreeMap<>();
      for (ProducerRecord<byte[], byte[]> r : producer.history()) {
        String[] parts = new String(r.value(), StandardCharsets.UTF_8).split(":");
        byKey
            .computeIfAbsent(parts[0], k -> new java.util.ArrayList<>())
            .add(Long.parseLong(parts[1]));
      }
      for (List<Long> offsets : byKey.values()) {
        assertEquals(perKey, offsets.size());
        assertEquals(offsets.stream().sorted().toList(), offsets, "per-key output order");
      }
    }

    @Test
    void processorAndSinkAreClosedAtShutdown_userDlqHandlerIsNot() throws Exception {
      MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
      AtomicInteger processorClosed = new AtomicInteger();
      AtomicInteger sinkClosed = new AtomicInteger();
      AtomicInteger dlqClosed = new AtomicInteger();
      RecordProcessor<String, String, String> processor =
          new RecordProcessor<>() {
            @Override
            public Optional<String> process(ConsumerRecord<String, String> record) {
              return Optional.of(record.value());
            }

            @Override
            public void close() {
              processorClosed.incrementAndGet();
            }
          };
      RecordSink<String> sink =
          new RecordSink<>() {
            @Override
            public java.util.concurrent.CompletionStage<Void> write(
                SinkContext ctx, String output) {
              return java.util.concurrent.CompletableFuture.completedFuture(null);
            }

            @Override
            public void close() {
              sinkClosed.incrementAndGet();
            }
          };
      class ClosingDlq
          implements io.github.openlogiclab.kafkapipeline.error.DLQHandler<String, String>,
              AutoCloseable {
        @Override
        public void send(ConsumerRecord<String, String> record, Exception lastError) {}

        @Override
        public void close() {
          dlqClosed.incrementAndGet();
        }
      }
      pipeline =
          new KafkaPipeline<>(
              builder()
                  .processor(processor)
                  .sink(sink)
                  .errorStrategy(
                      ErrorStrategy.<String, String>builder().dlqHandler(new ClosingDlq()).build())
                  .build(),
              consumer);
      pipeline.startAsync();
      assertTrue(await(pipeline::isRunning));
      pipeline.stop();
      pipeline.awaitShutdown();
      assertEquals(1, processorClosed.get());
      assertEquals(1, sinkClosed.get());
      assertEquals(
          0, dlqClosed.get(), "the DLQ handler belongs to the ErrorStrategy; its owner closes it");
    }
  }

  @Nested
  class Validation {

    @Test
    void handlerAndProcessorAreMutuallyExclusive() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              builder()
                  .handler(r -> {})
                  .processor(r -> Optional.of("x"))
                  .sink((ctx, out) -> java.util.concurrent.CompletableFuture.completedFuture(null))
                  .build());
    }

    @Test
    void sinkOfASupertypeIsAccepted() {
      RecordSink<Number> numbers =
          (ctx, out) -> java.util.concurrent.CompletableFuture.completedFuture(null);
      assertDoesNotThrow(
          () ->
              builder()
                  .processor(r -> Optional.of(Integer.valueOf(r.value().length())))
                  .sink(numbers)
                  .build());
    }

    @Test
    void bufferingSinkMustFitTheWindow() {
      RecordSink<String> buffering =
          new RecordSink<>() {
            @Override
            public java.util.concurrent.CompletionStage<Void> write(
                SinkContext ctx, String output) {
              return java.util.concurrent.CompletableFuture.completedFuture(null);
            }

            @Override
            public OptionalInt maxBufferedRecordsPerPartition() {
              return OptionalInt.of(9_000);
            }
          };
      assertThrows(
          IllegalArgumentException.class,
          () -> builder().processor(r -> Optional.of(r.value())).sink(buffering).build(),
          "9,000 >= 80% of the default 10,000 window");
      assertDoesNotThrow(
          () ->
              builder()
                  .maxWindowSize(20_000)
                  .processor(r -> Optional.of(r.value()))
                  .sink(buffering)
                  .build());
    }
  }
}
