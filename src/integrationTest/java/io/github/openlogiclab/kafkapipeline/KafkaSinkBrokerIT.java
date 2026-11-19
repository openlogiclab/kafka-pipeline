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
import io.github.openlogiclab.kafkapipeline.sink.kafka.KafkaDLQHandler;
import io.github.openlogiclab.kafkapipeline.sink.kafka.KafkaHeaders;
import io.github.openlogiclab.kafkapipeline.sink.kafka.KafkaRecordSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Processor + Kafka sink against a real broker: nothing lost, per-key order kept, DLQ works. */
@Testcontainers(disabledWithoutDocker = true)
class KafkaSinkBrokerIT {

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0");

  private static final int PARTITIONS = 4;

  private final List<KafkaPipeline<String, String>> pipelines = new ArrayList<>();

  @AfterEach
  void stopAll() throws InterruptedException {
    for (KafkaPipeline<String, String> p : pipelines) {
      p.stop();
      p.awaitShutdown();
    }
  }

  @Test
  void everyInputIsWrittenAndCommitted_withItsSourceHeader() throws Exception {
    String in = createTopic();
    String out = createTopic();
    produce(in, 2_000, i -> "k" + i);

    start(in, out, ProcessingOrder.UNORDERED, b -> {});

    List<ConsumerRecord<String, String>> written = readUntil(out, 2_000);
    Set<String> sources = ConcurrentHashMap.newKeySet();
    for (ConsumerRecord<String, String> r : written) {
      Header h = r.headers().lastHeader(KafkaHeaders.SOURCE);
      assertNotNull(h);
      sources.add(new String(h.value(), StandardCharsets.UTF_8));
    }
    assertEquals(2_000, sources.size(), "each input produced an output");
  }

  @Test
  void keyOrdering_outputsOfEachKeyAreInInputOrder() throws Exception {
    String in = createTopic();
    String out = createTopic();
    produce(in, 3_000, i -> "key" + (i % 30));

    start(in, out, ProcessingOrder.KEY, b -> {});

    // Same key in, same key out, so each key's outputs land in one output partition, in order.
    Map<String, List<Integer>> seqByKey = new HashMap<>();
    for (ConsumerRecord<String, String> r : readUntil(out, 3_000)) {
      int seq = Integer.parseInt(r.value().substring(r.value().lastIndexOf('-') + 1));
      seqByKey.computeIfAbsent(r.key(), k -> new ArrayList<>()).add(seq);
    }
    assertEquals(30, seqByKey.size());
    seqByKey.forEach(
        (key, seqs) -> assertEquals(seqs.stream().sorted().toList(), seqs, "order of " + key));
  }

  @Test
  void restartMidStream_losesNoOutput() throws Exception {
    String in = createTopic();
    String out = createTopic();
    String group = "g-" + UUID.randomUUID();
    produce(in, 3_000, i -> "k" + i);

    KafkaPipeline<String, String> first = start(group, in, out, ProcessingOrder.UNORDERED, b -> {});
    Thread.sleep(300);
    first.stop();
    first.awaitShutdown();
    start(group, in, out, ProcessingOrder.UNORDERED, b -> {});

    // At-least-once: every input has an output; a few may appear twice after the restart.
    Set<String> values = ConcurrentHashMap.newKeySet();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    try (KafkaConsumer<String, String> reader = reader(out)) {
      while (values.size() < 3_000 && System.nanoTime() < deadline) {
        reader.poll(Duration.ofMillis(200)).forEach(r -> values.add(r.value()));
      }
    }
    assertEquals(3_000, values.size());
  }

  @Test
  void recordsThatAlwaysFail_goToTheDlqAndTheRestFlows() throws Exception {
    String in = createTopic();
    String out = createTopic();
    String dlqTopic = createTopic();
    produce(in, 500, i -> "k" + i);

    start(
        "g-" + UUID.randomUUID(),
        in,
        out,
        ProcessingOrder.UNORDERED,
        b ->
            b.errorStrategy(
                ErrorStrategy.<String, String>builder()
                    .maxRetries(1)
                    .retryBackoff(Duration.ofMillis(10))
                    .dlqHandler(
                        KafkaDLQHandler.<String, String>builder()
                            .producerProperties(producerProps())
                            .topic(dlqTopic)
                            .keySerializer(new StringSerializer())
                            .valueSerializer(new StringSerializer())
                            .build())
                    .build()),
        value -> {
          if (value.endsWith("-7")) throw new IllegalStateException("poison");
        });

    assertEquals(499, readUntil(out, 499).size());
    List<ConsumerRecord<String, String>> dead = readUntil(dlqTopic, 1);
    assertEquals("v-7", dead.get(0).value());
    assertNotNull(dead.get(0).headers().lastHeader(KafkaHeaders.DLQ_ERROR_CLASS));
  }

  // ── helpers ───────────────────────────────────────────────────

  private KafkaPipeline<String, String> start(
      String in,
      String out,
      ProcessingOrder order,
      java.util.function.Consumer<PipelineConfig.Builder<String, String>> customize) {
    return start("g-" + UUID.randomUUID(), in, out, order, customize);
  }

  private KafkaPipeline<String, String> start(
      String group,
      String in,
      String out,
      ProcessingOrder order,
      java.util.function.Consumer<PipelineConfig.Builder<String, String>> customize) {
    return start(group, in, out, order, customize, value -> {});
  }

  interface Check {
    void check(String value) throws Exception;
  }

  private KafkaPipeline<String, String> start(
      String group,
      String in,
      String out,
      ProcessingOrder order,
      java.util.function.Consumer<PipelineConfig.Builder<String, String>> customize,
      Check check) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    KafkaRecordSink<String, String> sink =
        KafkaRecordSink.<String, String>builder()
            .producerProperties(producerProps)
            .topic(out)
            .keyFrom(v -> v.substring(0, v.indexOf('|')))
            .keySerializer(new StringSerializer())
            .valueSerializer(
                (topic, v) -> v.substring(v.indexOf('|') + 1).getBytes(StandardCharsets.UTF_8))
            .build();

    PipelineConfig.Builder<String, String> builder =
        KafkaPipeline.<String, String>builder()
            .consumerProperties(props)
            .topic(in)
            .concurrency(ThreadMode.VIRTUAL, 100)
            .ordering(order)
            .processor(
                record -> {
                  Thread.sleep(ThreadLocalRandom.current().nextInt(0, 3)); // simulated I/O
                  check.check(record.value());
                  // carry the key through: "key|value"
                  return List.of(record.key() + "|" + record.value());
                })
            .sink(sink)
            .commitInterval(Duration.ofMillis(200))
            .drainTimeout(Duration.ofSeconds(2))
            .shutdownTimeout(Duration.ofSeconds(10));
    customize.accept(builder);
    KafkaPipeline<String, String> pipeline = new KafkaPipeline<>(builder.build());
    pipelines.add(pipeline);
    pipeline.startAsync();
    return pipeline;
  }

  private static String createTopic() throws Exception {
    String topic = "t-" + UUID.randomUUID();
    try (Admin admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
    }
    return topic;
  }

  private static Properties producerProps() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }

  private static void produce(String topic, int count, java.util.function.IntFunction<String> key)
      throws Exception {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps())) {
      for (int i = 0; i < count; i++) {
        producer.send(new ProducerRecord<>(topic, key.apply(i), "v-" + i));
      }
      producer.flush();
    }
  }

  private static KafkaConsumer<String, String> reader(String topic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  /** Reads the topic until it holds at least {@code count} records (or fails after 60 s). */
  private static List<ConsumerRecord<String, String>> readUntil(String topic, int count)
      throws InterruptedException {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    await(
        () -> {
          try (KafkaConsumer<String, String> reader = reader(topic)) {
            records.clear();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (records.size() < count && System.nanoTime() < until) {
              reader.poll(Duration.ofMillis(200)).forEach(records::add);
            }
          }
          return records.size() >= count;
        },
        () -> topic + " has " + records.size() + "/" + count);
    return records;
  }

  private static void await(BooleanSupplier condition, Supplier<String> message)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail(message.get());
      Thread.sleep(100);
    }
  }
}
