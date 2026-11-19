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
import io.github.openlogiclab.kafkapipeline.error.KafkaDLQHandler;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.kafka.KafkaRecordSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** A whole pipeline writing through the Kafka sink and DLQ handler, with mock clients. */
class KafkaSinkPipelineTest {

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
    assertTrue(await(() -> consumer.subscription().contains(TOPIC)));
    consumer.updateBeginningOffsets(Map.of(TP0, 0L));
    consumer.rebalance(List.of(TP0));
  }

  @Test
  void processesWritesToKafkaAndCommits() throws Exception {
    MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    MockProducer<byte[], byte[]> producer = producer();
    KafkaRecordSink<String, String> sink =
        KafkaRecordSink.<String, String>builder()
            .producer(producer)
            .topic("orders-enriched")
            .keyFrom(v -> v.substring(0, 2))
            .keySerializer(new StringSerializer())
            .valueSerializer(new StringSerializer())
            .build();
    pipeline =
        new KafkaPipeline<>(
            builder()
                .processor(
                    r ->
                        r.value().startsWith("skip")
                            ? Optional.empty()
                            : Optional.of(r.value() + "!"))
                .sink(sink)
                .build(),
            consumer);
    pipeline.startAsync();
    // isRunning() is true from startAsync() on; rebalance() needs the subscription.
    assertTrue(await(() -> !consumer.subscription().isEmpty()));
    assign(consumer);
    for (int i = 0; i < 20; i++) {
      consumer.addRecord(
          new ConsumerRecord<>(TOPIC, 0, i, "k" + i, (i == 5 ? "skip" : "o" + i) + "-" + i));
    }

    assertTrue(await(() -> committed(consumer) == 20), "all 20 committed after their writes");
    List<ProducerRecord<byte[], byte[]>> sent = producer.history();
    assertEquals(19, sent.size(), "one record produced nothing");
    // UNORDERED: every output is written once, in any order.
    assertTrue(
        sent.stream()
            .map(r -> new String(r.value(), StandardCharsets.UTF_8))
            .anyMatch("o0-0!"::equals));
    assertNotNull(sent.get(0).headers().lastHeader(KafkaHeaders.SOURCE));
  }

  @Test
  void failingWrites_endInTheKafkaDlq() throws Exception {
    MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    MockProducer<byte[], byte[]> dlqProducer = producer();
    KafkaDLQHandler<String, String> dlq =
        KafkaDLQHandler.<String, String>builder()
            .producer(dlqProducer)
            .topic("orders-dlq")
            .keySerializer(new StringSerializer())
            .valueSerializer(new StringSerializer())
            .build();
    List<String> written = new CopyOnWriteArrayList<>();
    RecordSink<String> sink =
        RecordSink.blocking(
            (ctx, output) -> {
              if (output.contains("bad")) throw new IllegalStateException("rejected");
              written.add(output);
            });
    pipeline =
        new KafkaPipeline<>(
            builder()
                .processor(r -> Optional.of(r.value()))
                .sink(sink)
                .errorStrategy(
                    ErrorStrategy.<String, String>builder()
                        .maxRetries(2)
                        .retryBackoff(Duration.ofMillis(1))
                        .dlqHandler(dlq)
                        .build())
                .build(),
            consumer);
    pipeline.startAsync();
    // isRunning() is true from startAsync() on; rebalance() needs the subscription.
    assertTrue(await(() -> !consumer.subscription().isEmpty()));
    assign(consumer);
    consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", "good"));
    consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", "bad"));
    consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, "k", "good2"));

    assertTrue(await(() -> committed(consumer) == 3), "the bad record does not block");
    assertEquals(List.of("good", "good2"), written.stream().sorted().toList());
    assertEquals(1, dlqProducer.history().size());
    assertEquals("orders-dlq", dlqProducer.history().get(0).topic());
  }
}
