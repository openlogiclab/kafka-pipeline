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

import io.github.openlogiclab.kafkapipeline.error.DLQHandler;
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Sends records that failed for good to a dead-letter topic. Use it as the error strategy's {@code
 * dlqHandler}; the pipeline closes it at shutdown.
 *
 * <p>The original key and value are written again with the given serializers, so the DLQ record is
 * partitioned like the original. The original headers are kept, and these are added:
 *
 * <ul>
 *   <li>{@value KafkaHeaders#SOURCE}: the input record's id ({@code topic-partition-offset}), to
 *       tell apart duplicates a redelivery may cause
 *   <li>{@value KafkaHeaders#DLQ_TOPIC}, {@value KafkaHeaders#DLQ_PARTITION}, {@value
 *       KafkaHeaders#DLQ_OFFSET}: where it came from
 *   <li>{@value KafkaHeaders#DLQ_ERROR_CLASS}, {@value KafkaHeaders#DLQ_ERROR_MESSAGE}: the last
 *       error
 * </ul>
 *
 * <p>{@link #send} waits until the broker has the record, so a record counts as dead-lettered only
 * once it is really in the DLQ. Share the producer of the pipeline's {@link KafkaRecordSink} with
 * {@link Builder#sharing}, or give the DLQ its own.
 *
 * @param <K> input key type
 * @param <V> input value type
 */
public final class KafkaDLQHandler<K, V> implements DLQHandler<K, V>, AutoCloseable {

  private static final int MAX_MESSAGE_LENGTH = 1_000;

  private final Producer<byte[], byte[]> producer;
  private final boolean ownsProducer;
  private final String topic;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Duration sendTimeout;
  private final Duration closeTimeout;

  private KafkaDLQHandler(Builder<K, V> b) {
    if (b.producer != null) {
      this.producer = b.producer;
      this.ownsProducer = false;
    } else {
      this.producer = new KafkaProducer<>(KafkaRecordSink.producerConfig(b.producerProperties));
      this.ownsProducer = true;
    }
    this.topic = b.topic;
    this.keySerializer = b.keySerializer;
    this.valueSerializer = b.valueSerializer;
    this.sendTimeout = b.sendTimeout;
    this.closeTimeout = b.closeTimeout;
  }

  /**
   * Returns a new builder.
   *
   * @param <K> input key type
   * @param <V> input value type
   * @return the builder
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  @Override
  public void send(ConsumerRecord<K, V> record, Exception lastError) throws Exception {
    Headers headers = new RecordHeaders();
    for (Header header : record.headers()) {
      headers.add(header);
    }
    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
    headers.add(KafkaHeaders.SOURCE, utf8(SinkContext.recordId(tp, record.offset())));
    headers.add(KafkaHeaders.DLQ_TOPIC, utf8(record.topic()));
    headers.add(KafkaHeaders.DLQ_PARTITION, utf8(Integer.toString(record.partition())));
    headers.add(KafkaHeaders.DLQ_OFFSET, utf8(Long.toString(record.offset())));
    if (lastError != null) {
      headers.add(KafkaHeaders.DLQ_ERROR_CLASS, utf8(lastError.getClass().getName()));
      String message = String.valueOf(lastError.getMessage());
      if (message.length() > MAX_MESSAGE_LENGTH) message = message.substring(0, MAX_MESSAGE_LENGTH);
      headers.add(KafkaHeaders.DLQ_ERROR_MESSAGE, utf8(message));
    }
    byte[] key =
        record.key() == null ? null : keySerializer.serialize(topic, headers, record.key());
    byte[] value =
        record.value() == null ? null : valueSerializer.serialize(topic, headers, record.value());
    Future<RecordMetadata> sent;
    try {
      sent = producer.send(new ProducerRecord<>(topic, null, key, value, headers));
    } catch (IllegalStateException closed) {
      throw new ProducerUnusableException(closed);
    }
    try {
      sent.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      // Surface the broker's error itself, so classify() sees what went wrong.
      if (e.getCause() instanceof Exception cause) throw cause;
      throw e;
    } catch (java.util.concurrent.TimeoutException e) {
      // The broker did not answer in time: the DLQ is unavailable for now, not this record.
      throw new org.apache.kafka.common.errors.TimeoutException(
          "DLQ send to " + topic + " not acknowledged within " + sendTimeout, e);
    }
  }

  /**
   * Kafka's classification ({@link WriteFailure#of}), plus: a producer that can no longer send
   * (closed by its owner) is {@code FATAL}.
   */
  @Override
  public WriteFailure classify(Throwable error) {
    return error instanceof ProducerUnusableException ? WriteFailure.FATAL : WriteFailure.of(error);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** Closes the serializers, and the producer if this DLQ created it. */
  @Override
  public void close() {
    try {
      KafkaRecordSink.closeSerializer(keySerializer);
      KafkaRecordSink.closeSerializer(valueSerializer);
    } finally {
      if (ownsProducer) producer.close(closeTimeout);
    }
  }

  /**
   * Builder for {@link KafkaDLQHandler}.
   *
   * @param <K> input key type
   * @param <V> input value type
   */
  public static final class Builder<K, V> {
    private Properties producerProperties;
    private Producer<byte[], byte[]> producer;
    private String topic;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;
    private Duration sendTimeout = Duration.ofSeconds(30);
    private Duration closeTimeout = Duration.ofSeconds(30);

    private Builder() {}

    /**
     * Producer configuration for a DLQ-owned producer, closed at shutdown.
     *
     * @param props producer properties
     * @return this builder
     */
    public Builder<K, V> producerProperties(Properties props) {
      Properties copy = new Properties();
      copy.putAll(Objects.requireNonNull(props, "props"));
      this.producerProperties = copy;
      return this;
    }

    /**
     * Sends through the producer of the pipeline's output sink, which keeps owning it.
     *
     * @param sink the output sink
     * @return this builder
     */
    public Builder<K, V> sharing(KafkaRecordSink<?, ?> sink) {
      this.producer = Objects.requireNonNull(sink, "sink").producer();
      return this;
    }

    /**
     * Uses an existing producer; the DLQ does not close it.
     *
     * @param producer a producer of serialized bytes
     * @return this builder
     */
    public Builder<K, V> producer(Producer<byte[], byte[]> producer) {
      this.producer = Objects.requireNonNull(producer, "producer");
      return this;
    }

    /**
     * The dead-letter topic. Required.
     *
     * @param topic the topic
     * @return this builder
     */
    public Builder<K, V> topic(String topic) {
      this.topic = Objects.requireNonNull(topic, "topic");
      return this;
    }

    /**
     * Serializer for the original keys. Required.
     *
     * @param serializer the key serializer
     * @return this builder
     */
    public Builder<K, V> keySerializer(Serializer<K> serializer) {
      this.keySerializer = Objects.requireNonNull(serializer, "serializer");
      return this;
    }

    /**
     * Serializer for the original values. Required.
     *
     * @param serializer the value serializer
     * @return this builder
     */
    public Builder<K, V> valueSerializer(Serializer<V> serializer) {
      this.valueSerializer = Objects.requireNonNull(serializer, "serializer");
      return this;
    }

    /**
     * How long {@link KafkaDLQHandler#send} waits for the broker. Default: 30 seconds.
     *
     * @param timeout the send timeout
     * @return this builder
     */
    public Builder<K, V> sendTimeout(Duration timeout) {
      this.sendTimeout = Objects.requireNonNull(timeout, "timeout");
      return this;
    }

    /**
     * How long closing a DLQ-owned producer may take. Default: 30 seconds.
     *
     * @param timeout the close timeout
     * @return this builder
     */
    public Builder<K, V> closeTimeout(Duration timeout) {
      this.closeTimeout = Objects.requireNonNull(timeout, "timeout");
      return this;
    }

    /**
     * Builds the DLQ sink.
     *
     * @return the DLQ sink
     * @throws IllegalStateException if a required setting is missing
     */
    public KafkaDLQHandler<K, V> build() {
      if (producer == null && producerProperties == null) {
        throw new IllegalStateException(
            "Set producerProperties(...), sharing(...) or producer(...)");
      }
      if (producer != null && producerProperties != null) {
        throw new IllegalStateException("Set only one of producerProperties, sharing, producer");
      }
      if (topic == null) throw new IllegalStateException("Set topic(...)");
      if (keySerializer == null || valueSerializer == null) {
        throw new IllegalStateException("Set keySerializer(...) and valueSerializer(...)");
      }
      return new KafkaDLQHandler<>(this);
    }
  }
}
