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
package io.github.openlogiclab.kafkapipeline.sink.kafka;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.KafkaHeaders;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaRecordSinkTest {

  private static final SinkContext CTX =
      new SinkContext(new TopicPartition("in", 3), 42, new RecordHeaders());

  private static MockProducer<byte[], byte[]> producer(boolean autoComplete) {
    return new MockProducer<>(
        autoComplete, null, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private static KafkaRecordSink<String, String> sink(MockProducer<byte[], byte[]> producer) {
    return KafkaRecordSink.<String, String>builder()
        .producer(producer)
        .topic("out")
        .keyFrom(v -> "key-" + v)
        .keySerializer(new StringSerializer())
        .valueSerializer(new StringSerializer())
        .maxPendingSends(2)
        .build();
  }

  private static String header(ProducerRecord<byte[], byte[]> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  @Nested
  class Behaviour {

    @Test
    void stageCompletesOnlyWhenTheBrokerAcks() throws Exception {
      MockProducer<byte[], byte[]> producer = producer(false);
      KafkaRecordSink<String, String> sink = sink(producer);

      CompletableFuture<Void> stage = sink.write(CTX, "a").toCompletableFuture();
      assertFalse(stage.isDone());
      assertEquals(1, sink.pendingSends());

      assertTrue(producer.completeNext());
      stage.get(1, TimeUnit.SECONDS);
      assertEquals(0, sink.pendingSends());

      ProducerRecord<byte[], byte[]> first = producer.history().get(0);
      assertEquals("out", first.topic());
      assertEquals("key-a", new String(first.key(), StandardCharsets.UTF_8));
      assertEquals("a", new String(first.value(), StandardCharsets.UTF_8));
      assertEquals("in-3-42", header(first, KafkaHeaders.SOURCE));
      assertEquals("0", header(first, KafkaHeaders.OUTPUT_INDEX));
    }

    @Test
    void brokerError_failsTheStage() {
      MockProducer<byte[], byte[]> producer = producer(false);
      CompletableFuture<Void> stage = sink(producer).write(CTX, "a").toCompletableFuture();
      producer.errorNext(new IllegalStateException("not leader"));
      assertTrue(stage.isCompletedExceptionally());
    }

    @Test
    void serializationError_failsWithoutSendingAnything() {
      MockProducer<byte[], byte[]> producer = producer(true);
      Serializer<String> failing =
          (topic, value) -> {
            if (value.equals("bad")) throw new IllegalArgumentException("cannot serialize");
            return value.getBytes(StandardCharsets.UTF_8);
          };
      KafkaRecordSink<String, String> sink =
          KafkaRecordSink.<String, String>builder()
              .producer(producer)
              .topic("out")
              .valueSerializer(failing)
              .build();
      CompletableFuture<Void> stage = sink.write(CTX, "bad").toCompletableFuture();
      assertTrue(stage.isCompletedExceptionally());
      assertTrue(producer.history().isEmpty(), "nothing sent for the record");
      assertEquals(0, sink.pendingSends());
    }

    @Test
    void pendingSends_throttleGloballyWithHysteresis() {
      MockProducer<byte[], byte[]> producer = producer(false);
      KafkaRecordSink<String, String> sink = sink(producer); // high 2, low 1
      var sensor = sink.sensor().orElseThrow();
      sink.write(CTX, "a");
      assertEquals(BackpressureStatus.OK, sensor.evaluate());
      sink.write(CTX, "b");
      assertEquals(BackpressureStatus.THROTTLE, sensor.evaluate());
      producer.completeNext(); // 1 pending: at the low mark
      assertEquals(BackpressureStatus.OK, sensor.evaluate());
    }

    @Test
    void forEach_sendsInListOrderAndAcksWhenAllLand() throws Exception {
      MockProducer<byte[], byte[]> producer = producer(false);
      KafkaRecordSink<String, String> sink = sink(producer);
      CompletableFuture<Void> stage =
          RecordSink.forEach(sink).write(CTX, List.of("a", "b")).toCompletableFuture();
      assertEquals(2, sink.pendingSends());
      assertEquals(
          List.of("a", "b"),
          producer.history().stream()
              .map(r -> new String(r.value(), StandardCharsets.UTF_8))
              .toList());
      assertEquals(
          List.of("0", "1"),
          producer.history().stream().map(r -> header(r, KafkaHeaders.OUTPUT_INDEX)).toList(),
          "outputs of one record share kp-source and differ in kp-output-index");

      assertTrue(producer.completeNext());
      assertFalse(stage.isDone(), "one of two outputs acked");
      assertTrue(producer.completeNext());
      stage.get(1, TimeUnit.SECONDS);
    }

    @Test
    void forEach_oneFailedOutputFailsTheWholeRecordEvenIfAnotherLanded() {
      MockProducer<byte[], byte[]> producer = producer(false);
      CompletableFuture<Void> stage =
          RecordSink.forEach(sink(producer)).write(CTX, List.of("a", "b")).toCompletableFuture();
      assertTrue(producer.completeNext(), "a landed");
      assertFalse(stage.isDone());
      assertTrue(producer.errorNext(new org.apache.kafka.common.errors.RecordTooLargeException()));
      assertTrue(stage.isCompletedExceptionally(), "the record is not acked on partial output");
    }

    @Test
    void sendRejectedSynchronously_failsTheStageAndKeepsTheCountExact() {
      MockProducer<byte[], byte[]> producer = producer(true);
      KafkaRecordSink<String, String> sink = sink(producer);
      producer.close();
      CompletableFuture<Void> stage = sink.write(CTX, "a").toCompletableFuture();
      assertTrue(stage.isCompletedExceptionally());
      assertEquals(0, sink.pendingSends());
    }

    @Test
    void flush_pushesQueuedSendsOut() throws Exception {
      MockProducer<byte[], byte[]> producer = producer(false);
      KafkaRecordSink<String, String> sink = sink(producer);
      CompletableFuture<Void> stage = sink.write(CTX, "a").toCompletableFuture();
      sink.flush(CTX.partition()).toCompletableFuture().get(5, TimeUnit.SECONDS);
      stage.get(5, TimeUnit.SECONDS);
      assertEquals(0, sink.pendingSends());
    }

    @Test
    void onPartitionRevoked_flushesWhatTheProducerHolds() throws Exception {
      MockProducer<byte[], byte[]> producer = producer(false);
      KafkaRecordSink<String, String> sink = sink(producer);
      CompletableFuture<Void> stage = sink.write(CTX, "held").toCompletableFuture();
      sink.onPartitionRevoked(CTX.partition(), java.time.Duration.ofSeconds(30));
      stage.get(5, TimeUnit.SECONDS); // completed by the flush, not by anyone completing the send
    }

    @Test
    void everyFlushRequestCoversTheSendsMadeBeforeIt() throws Exception {
      MockProducer<byte[], byte[]> producer = producer(false);
      KafkaRecordSink<String, String> sink = sink(producer);
      for (int i = 0; i < 50; i++) {
        CompletableFuture<Void> stage = sink.write(CTX, "v" + i).toCompletableFuture();
        sink.flush(CTX.partition()).toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertTrue(stage.isDone(), "send " + i + " was made before its flush request");
      }
    }

    @Test
    void flushRequestedWhileAFlushRuns_getsAFlushOfItsOwn() throws Exception {
      java.util.concurrent.CountDownLatch inFirstFlush = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.CountDownLatch releaseFirstFlush =
          new java.util.concurrent.CountDownLatch(1);
      AtomicInteger flushes = new AtomicInteger();
      MockProducer<byte[], byte[]> producer =
          new MockProducer<>(false, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public void flush() {
              super.flush(); // completes the sends made so far, then releases the lock
              if (flushes.incrementAndGet() == 1) {
                inFirstFlush.countDown();
                try {
                  releaseFirstFlush.await(); // still "flushing" while a new send arrives
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            }
          };
      KafkaRecordSink<String, String> sink = sink(producer);
      CompletableFuture<Void> first = sink.write(CTX, "a").toCompletableFuture();
      CompletableFuture<Void> firstFlush = sink.flush(CTX.partition()).toCompletableFuture();
      assertTrue(inFirstFlush.await(5, TimeUnit.SECONDS));

      // A send and a flush request while the first flush is still running.
      CompletableFuture<Void> second = sink.write(CTX, "b").toCompletableFuture();
      CompletableFuture<Void> secondFlush = sink.flush(CTX.partition()).toCompletableFuture();
      assertFalse(second.isDone(), "the running flush started before this send");
      releaseFirstFlush.countDown();

      firstFlush.get(5, TimeUnit.SECONDS);
      first.get(5, TimeUnit.SECONDS);
      secondFlush.get(5, TimeUnit.SECONDS);
      assertTrue(second.isDone(), "the later request was served by a flush that covered its send");
    }

    @Test
    void closedProducer_isFatal() {
      MockProducer<byte[], byte[]> producer = producer(true);
      KafkaRecordSink<String, String> sink = sink(producer);
      producer.close();
      CompletableFuture<Void> stage = sink.write(CTX, "a").toCompletableFuture();
      Throwable error = assertThrows(java.util.concurrent.ExecutionException.class, stage::get);
      assertEquals(
          io.github.openlogiclab.kafkapipeline.error.WriteFailure.FATAL,
          sink.classify(error.getCause()));
    }

    @Test
    void classify_usesKafkasDefaults() {
      KafkaRecordSink<String, String> sink = sink(producer(true));
      assertEquals(
          io.github.openlogiclab.kafkapipeline.error.WriteFailure.TRANSIENT,
          sink.classify(new org.apache.kafka.common.errors.TimeoutException("t")));
      assertEquals(
          io.github.openlogiclab.kafkapipeline.error.WriteFailure.FATAL,
          sink.classify(new org.apache.kafka.common.errors.TopicAuthorizationException("acl")));
      assertEquals(
          io.github.openlogiclab.kafkapipeline.error.WriteFailure.RECORD,
          sink.classify(new org.apache.kafka.common.errors.RecordTooLargeException()));
    }

    @Test
    void close_closesTheSerializers() {
      AtomicInteger closed = new AtomicInteger();
      Serializer<String> closing =
          new Serializer<>() {
            @Override
            public byte[] serialize(String topic, String data) {
              return data.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void close() {
              closed.incrementAndGet();
            }
          };
      KafkaRecordSink.<String, String>builder()
          .producer(producer(true))
          .topic("out")
          .keyFrom(v -> v)
          .keySerializer(closing)
          .valueSerializer(closing)
          .build()
          .close();
      assertEquals(2, closed.get());
    }

    @Test
    void hugeMaxPendingSends_doesNotOverflowTheResumeMark() {
      KafkaRecordSink<String, String> sink =
          KafkaRecordSink.<String, String>builder()
              .producer(producer(true))
              .topic("out")
              .valueSerializer(new StringSerializer())
              .maxPendingSends(Long.MAX_VALUE)
              .build();
      assertEquals(BackpressureStatus.OK, sink.sensor().orElseThrow().evaluate());
    }

    @Test
    void producerPassedIn_isNotClosed() {
      MockProducer<byte[], byte[]> producer = producer(true);
      sink(producer).close();
      assertFalse(producer.closed());
    }

    @Test
    void builderValidation() {
      assertThrows(
          IllegalStateException.class,
          () -> KafkaRecordSink.<String, String>builder().topic("t").build(),
          "needs a producer");
      assertThrows(
          IllegalStateException.class,
          () ->
              KafkaRecordSink.<String, String>builder()
                  .producer(producer(true))
                  .topic("t")
                  .keyFrom(v -> v)
                  .valueSerializer(new StringSerializer())
                  .build(),
          "keyFrom needs a key serializer");
      Properties props = new Properties();
      props.put("acks", "1");
      Properties config = KafkaRecordSink.producerConfig(props);
      assertEquals("1", config.get("acks"), "user setting wins");
      assertEquals("true", config.get("enable.idempotence"));
      assertEquals(ByteArraySerializer.class.getName(), config.get("value.serializer"));
    }
  }
}
