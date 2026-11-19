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
package io.github.openlogiclab.kafkapipeline.error;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.KafkaHeaders;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaDLQHandlerTest {

  private static MockProducer<byte[], byte[]> producer(boolean autoComplete) {
    return new MockProducer<>(
        autoComplete, null, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private static String header(ProducerRecord<byte[], byte[]> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  @Test
  void sendsOriginalRecordWithErrorHeaders() throws Exception {
    MockProducer<byte[], byte[]> producer = producer(true);
    KafkaDLQHandler<String, String> dlq =
        KafkaDLQHandler.<String, String>builder()
            .producer(producer)
            .topic("dlq")
            .keySerializer(new StringSerializer())
            .valueSerializer(new StringSerializer())
            .build();
    ConsumerRecord<String, String> record = new ConsumerRecord<>("in", 1, 7L, "k", "v");
    record.headers().add("trace", "t1".getBytes(StandardCharsets.UTF_8));

    dlq.send(record, new IllegalStateException("boom"));

    ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
    assertEquals("dlq", sent.topic());
    assertEquals("k", new String(sent.key(), StandardCharsets.UTF_8));
    assertEquals("v", new String(sent.value(), StandardCharsets.UTF_8));
    assertEquals("t1", header(sent, "trace"), "original headers kept");
    assertEquals("in-1-7", header(sent, KafkaHeaders.SOURCE));
    assertEquals("in", header(sent, KafkaHeaders.DLQ_TOPIC));
    assertEquals("1", header(sent, KafkaHeaders.DLQ_PARTITION));
    assertEquals("7", header(sent, KafkaHeaders.DLQ_OFFSET));
    assertEquals(IllegalStateException.class.getName(), header(sent, KafkaHeaders.DLQ_ERROR_CLASS));
    assertEquals("boom", header(sent, KafkaHeaders.DLQ_ERROR_MESSAGE));
    dlq.close();
    assertFalse(producer.closed(), "a producer passed in belongs to its owner");
  }

  @Test
  void longErrorMessage_isCutTo1000Characters() throws Exception {
    MockProducer<byte[], byte[]> producer = producer(true);
    KafkaDLQHandler<String, String> dlq =
        KafkaDLQHandler.<String, String>builder()
            .producer(producer)
            .topic("dlq")
            .keySerializer(new StringSerializer())
            .valueSerializer(new StringSerializer())
            .build();
    dlq.send(new ConsumerRecord<>("in", 0, 0L, "k", "v"), new RuntimeException("x".repeat(5000)));
    assertEquals(1000, header(producer.history().get(0), KafkaHeaders.DLQ_ERROR_MESSAGE).length());
  }

  @Test
  void brokerError_throwsSoTheFinalFailureHandlerRuns() {
    MockProducer<byte[], byte[]> producer = producer(false);
    KafkaDLQHandler<String, String> dlq =
        KafkaDLQHandler.<String, String>builder()
            .producer(producer)
            .topic("dlq")
            .keySerializer(new StringSerializer())
            .valueSerializer(new StringSerializer())
            .sendTimeout(java.time.Duration.ofMillis(50))
            .build();
    Exception e =
        assertThrows(
            org.apache.kafka.common.errors.TimeoutException.class,
            () -> dlq.send(new ConsumerRecord<>("in", 0, 0L, "k", "v"), new RuntimeException()));
    assertEquals(
        io.github.openlogiclab.kafkapipeline.error.WriteFailure.TRANSIENT,
        dlq.classify(e),
        "an unanswered DLQ is unavailable, not a bad record");
  }

  @Test
  void closedProducer_isFatal() {
    MockProducer<byte[], byte[]> producer = producer(true);
    KafkaDLQHandler<String, String> dlq =
        KafkaDLQHandler.<String, String>builder()
            .producer(producer)
            .topic("dlq")
            .keySerializer(new StringSerializer())
            .valueSerializer(new StringSerializer())
            .build();
    producer.close();
    Exception e =
        assertThrows(
            ProducerUnusableException.class,
            () -> dlq.send(new ConsumerRecord<>("in", 0, 0L, "k", "v"), new RuntimeException()));
    assertEquals(WriteFailure.FATAL, dlq.classify(e));
  }

  @Test
  void builderRejectsTimeoutsThatWouldBreakSendingOrClosing() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaDLQHandler.<String, String>builder().sendTimeout(java.time.Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaDLQHandler.<String, String>builder()
                .closeTimeout(java.time.Duration.ofSeconds(-1)));
  }
}
