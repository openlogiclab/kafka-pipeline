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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Runs the pipeline against a real broker, covering what {@code MockConsumer} cannot: real fetch
 * and commit behaviour, consumer-group rebalances between two members, and resuming from committed
 * offsets after a restart.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaPipelineBrokerIT {

  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0");

  private static final int PARTITIONS = 4;

  private final List<KafkaPipeline<String, String>> pipelines = new java.util.ArrayList<>();

  @AfterEach
  void stopAll() throws InterruptedException {
    for (KafkaPipeline<String, String> p : pipelines) {
      p.stop();
      p.awaitShutdown();
    }
  }

  @Test
  void outOfOrderProcessing_processesEveryRecordAndCommitsToTheEnd() throws Exception {
    String topic = createTopic();
    String group = "g-" + UUID.randomUUID();
    produce(topic, 0, 2_000);

    Map<String, AtomicInteger> seen = new ConcurrentHashMap<>();
    start(
        group,
        topic,
        record -> {
          // Random latency so records finish out of order within each partition.
          Thread.sleep(ThreadLocalRandom.current().nextInt(0, 5));
          seen.computeIfAbsent(record.value(), v -> new AtomicInteger()).incrementAndGet();
        });

    await(() -> seen.size() == 2_000, () -> "processed " + seen.size() + "/2000");
    await(() -> committedToEnd(group, topic), () -> "committed " + committed(group, topic));
    assertTrue(
        seen.values().stream().allMatch(c -> c.get() == 1),
        "no rebalance or restart happened, so nothing should be processed twice");
  }

  @Test
  void secondMemberJoiningMidStream_noRecordIsLost() throws Exception {
    String topic = createTopic();
    String group = "g-" + UUID.randomUUID();
    Set<String> seen = ConcurrentHashMap.newKeySet();
    Map<String, Set<Integer>> partitionsByMember = new ConcurrentHashMap<>();

    start(group, topic, record -> handleAs("a", record, seen, partitionsByMember));
    produce(topic, 0, 1_000);
    await(() -> seen.size() >= 200, () -> "first member processed " + seen.size());

    start(group, topic, record -> handleAs("b", record, seen, partitionsByMember));
    await(() -> membersWithPartitions(group) == 2, () -> "second member never got partitions");
    produce(topic, 1_000, 3_000);

    await(() -> seen.size() == 3_000, () -> "processed " + seen.size() + "/3000");
    await(() -> committedToEnd(group, topic), () -> "committed " + committed(group, topic));
    assertEquals(
        Set.of("a", "b"),
        partitionsByMember.keySet(),
        "both members should have received partitions after the rebalance");
  }

  @Test
  void restart_resumesFromCommittedOffsets() throws Exception {
    String topic = createTopic();
    String group = "g-" + UUID.randomUUID();
    produce(topic, 0, 500);

    Set<String> firstRun = ConcurrentHashMap.newKeySet();
    KafkaPipeline<String, String> first =
        start(group, topic, record -> firstRun.add(record.value()));
    await(() -> firstRun.size() == 500, () -> "first run processed " + firstRun.size());
    await(() -> committedToEnd(group, topic), () -> "committed " + committed(group, topic));
    first.stop();
    first.awaitShutdown();
    pipelines.remove(first);

    produce(topic, 500, 800);
    Set<String> secondRun = ConcurrentHashMap.newKeySet();
    start(group, topic, record -> secondRun.add(record.value()));

    await(() -> secondRun.size() == 300, () -> "second run processed " + secondRun.size());
    Thread.sleep(500);
    assertEquals(
        rangeValues(500, 800),
        secondRun,
        "a restart should pick up exactly where the committed offsets left off");
  }

  // ── helpers ───────────────────────────────────────────────────

  private static void handleAs(
      String member,
      org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record,
      Set<String> seen,
      Map<String, Set<Integer>> partitionsByMember)
      throws InterruptedException {
    Thread.sleep(1);
    partitionsByMember
        .computeIfAbsent(member, m -> ConcurrentHashMap.newKeySet())
        .add(record.partition());
    seen.add(record.value());
  }

  private KafkaPipeline<String, String> start(
      String group,
      String topic,
      io.github.openlogiclab.kafkapipeline.handler.RecordHandler<String, String> handler) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    KafkaPipeline<String, String> pipeline =
        new KafkaPipeline<>(
            KafkaPipeline.<String, String>builder()
                .consumerProperties(props)
                .topic(topic)
                .handler(handler)
                .concurrency(ThreadMode.VIRTUAL, 50)
                .commitInterval(Duration.ofMillis(200))
                .shutdownTimeout(Duration.ofSeconds(10))
                .build());
    pipelines.add(pipeline);
    pipeline.startAsync();
    return pipeline;
  }

  private static String createTopic() throws Exception {
    String topic = "t-" + UUID.randomUUID();
    try (Admin admin = admin()) {
      admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
    }
    return topic;
  }

  private static void produce(String topic, int from, int to) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = from; i < to; i++) {
        producer.send(new ProducerRecord<>(topic, "k" + i, "v" + i));
      }
      producer.flush();
    }
  }

  private static Admin admin() {
    return Admin.create(
        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
  }

  private static Map<TopicPartition, Long> committed(String group, String topic) {
    try (Admin admin = admin()) {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
      Map<TopicPartition, Long> result = new HashMap<>();
      offsets.forEach(
          (tp, om) -> {
            if (tp.topic().equals(topic) && om != null) result.put(tp, om.offset());
          });
      return result;
    } catch (Exception e) {
      return Map.of();
    }
  }

  private static boolean committedToEnd(String group, String topic) {
    try (Admin admin = admin()) {
      Map<TopicPartition, OffsetSpec> request = new HashMap<>();
      for (int p = 0; p < PARTITIONS; p++)
        request.put(new TopicPartition(topic, p), OffsetSpec.latest());
      Map<TopicPartition, Long> end =
          admin.listOffsets(request).all().get().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
      return end.equals(committed(group, topic));
    } catch (Exception e) {
      return false;
    }
  }

  private static int membersWithPartitions(String group) {
    try (Admin admin = admin()) {
      return (int)
          admin.describeConsumerGroups(List.of(group)).all().get().get(group).members().stream()
              .filter(m -> !m.assignment().topicPartitions().isEmpty())
              .count();
    } catch (Exception e) {
      return 0;
    }
  }

  private static Set<String> rangeValues(int from, int to) {
    Set<String> values = new java.util.HashSet<>();
    for (int i = from; i < to; i++) values.add("v" + i);
    return values;
  }

  private static void await(BooleanSupplier condition, java.util.function.Supplier<String> message)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail(message.get());
      Thread.sleep(100);
    }
  }
}
