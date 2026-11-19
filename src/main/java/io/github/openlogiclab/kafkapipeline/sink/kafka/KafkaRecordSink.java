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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Writes outputs to Kafka through one producer shared by all workers.
 *
 * <p>Workers call {@link #write} concurrently and return at once; the producer batches the sends
 * and keeps many in flight. A record is acked when every one of its outputs is acknowledged by the
 * broker, so its offset is never committed before its outputs are written (at-least-once). By
 * default each output carries a {@value KafkaHeaders#SOURCE} header with the input record's id, so
 * consumers can drop the duplicates a redelivery may produce.
 *
 * <p>Sends the producer has not completed yet feed the pipeline's global backpressure: above {@code
 * maxPendingSends} the pipeline stops fetching until they drop to 60% of it.
 *
 * <p>Outputs are serialized on the worker before sending, so a serialization error fails only that
 * record's write (it is retried, then the record goes to the DLQ or is dropped) and sends nothing
 * for it. Serializers are called concurrently and must be thread-safe, as the producer already
 * requires; the sink closes them when it closes. When the producer's buffer is full, {@code send}
 * blocks the worker for up to {@code max.block.ms}; the pending-sends sensor is meant to throttle
 * the pipeline well before that.
 *
 * <p>Producer timeouts and other {@code RetriableException}s are transient: the write is retried
 * until the cluster recovers, so an outage holds records back instead of dropping them. A missing
 * ACL on the output topic is fatal and stops the pipeline. A retried write sends all of the
 * record's outputs again, so outputs that already landed are duplicated; use the {@value
 * KafkaHeaders#SOURCE} header to drop duplicates downstream.
 *
 * <pre>{@code
 * KafkaRecordSink<String, EnrichedOrder> sink =
 *     KafkaRecordSink.<String, EnrichedOrder>builder()
 *         .producerProperties(producerProps)
 *         .topic("orders-enriched")
 *         .keyFrom(EnrichedOrder::orderId)
 *         .keySerializer(new StringSerializer())
 *         .valueSerializer(new EnrichedOrderSerializer())
 *         .build();
 * }</pre>
 *
 * @param <KO> output key type
 * @param <O> output type, written as the record value
 */
public final class KafkaRecordSink<KO, O> implements RecordSink<O> {

  /** Pending sends at which a throttled pipeline resumes, as a percentage of maxPendingSends. */
  static final int RESUME_PERCENT = 60;

  /** Default {@link Builder#maxPendingSends}: {@value}. */
  public static final long DEFAULT_MAX_PENDING_SENDS = 10_000;

  private final Producer<byte[], byte[]> producer;
  private final boolean ownsProducer;
  private final Function<O, String> topicOf;
  private final Function<O, KO> keyOf; // null: records have no key
  private final Serializer<KO> keySerializer;
  private final Serializer<O> valueSerializer;
  private final boolean sourceHeader;
  private final Duration closeTimeout;
  private final AtomicLong pendingSends = new AtomicLong();
  private final List<CompletableFuture<Void>> flushRequests = new ArrayList<>();
  private boolean flusherRunning; // guarded by flushRequests
  private final BackpressureSensor sensor; // null when disabled

  private KafkaRecordSink(Builder<KO, O> b) {
    this.topicOf = b.topicOf;
    this.keyOf = b.keyOf;
    this.keySerializer = b.keySerializer;
    this.valueSerializer = b.valueSerializer;
    this.sourceHeader = b.sourceHeader;
    this.closeTimeout = b.closeTimeout;
    // Everything that can fail is built before the producer, which would otherwise leak its
    // threads and sockets.
    this.sensor =
        b.maxPendingSends > 0
            ? BackpressureSensor.watermark(
                "kafka-sink-pending-sends",
                pendingSends::get,
                b.maxPendingSends,
                percentOf(b.maxPendingSends, RESUME_PERCENT))
            : null;
    if (b.producer != null) {
      this.producer = b.producer;
      this.ownsProducer = false;
    } else {
      this.producer = new KafkaProducer<>(producerConfig(b.producerProperties));
      this.ownsProducer = true;
    }
  }

  /** {@code value * percent / 100} without overflowing for large values. */
  private static long percentOf(long value, int percent) {
    return value / 100 * percent + value % 100 * percent / 100;
  }

  /**
   * Producer settings for a sink-owned producer: bytes in and out (the sink serializes), and safe
   * defaults the user can still override: {@code acks=all} and idempotence, which keeps per-key
   * order across producer retries.
   */
  static Properties producerConfig(Properties props) {
    Properties config = new Properties();
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    config.putAll(props);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return config;
  }

  /**
   * Returns a new builder.
   *
   * @param <KO> output key type
   * @param <O> output type
   * @return the builder
   */
  public static <KO, O> Builder<KO, O> builder() {
    return new Builder<>();
  }

  @Override
  public CompletionStage<Void> write(SinkContext ctx, List<O> outputs) {
    // Serialize everything first, so a serialization error sends nothing for this record.
    @SuppressWarnings("unchecked")
    ProducerRecord<byte[], byte[]>[] records = new ProducerRecord[outputs.size()];
    try {
      for (int i = 0; i < records.length; i++) {
        records[i] = toProducerRecord(ctx, outputs.get(i));
      }
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }

    CompletableFuture<?>[] sends = new CompletableFuture<?>[records.length];
    for (int i = 0; i < records.length; i++) {
      CompletableFuture<Void> sent = new CompletableFuture<>();
      sends[i] = sent;
      pendingSends.incrementAndGet();
      try {
        producer.send(
            records[i],
            (metadata, error) -> {
              pendingSends.decrementAndGet();
              if (error == null) {
                sent.complete(null);
              } else {
                sent.completeExceptionally(error);
              }
            });
      } catch (RuntimeException e) {
        // Rejected before it was queued: buffer full past max.block.ms, or the producer can no
        // longer send at all. Outputs sent before it may still land; the retry may write them again
        // (at-least-once).
        pendingSends.decrementAndGet();
        sent.completeExceptionally(
            e instanceof IllegalStateException closed ? new ProducerUnusableException(closed) : e);
      }
    }
    return sends.length == 1 ? sends[0].thenApply(ignored -> null) : CompletableFuture.allOf(sends);
  }

  private ProducerRecord<byte[], byte[]> toProducerRecord(SinkContext ctx, O output) {
    String topic = Objects.requireNonNull(topicOf.apply(output), "topic for output");
    Headers headers = new RecordHeaders();
    if (sourceHeader) {
      headers.add(KafkaHeaders.SOURCE, ctx.recordId().getBytes(StandardCharsets.UTF_8));
    }
    byte[] key =
        keyOf == null ? null : keySerializer.serialize(topic, headers, keyOf.apply(output));
    byte[] value = valueSerializer.serialize(topic, headers, output);
    return new ProducerRecord<>(topic, null, key, value, headers);
  }

  /**
   * Kafka's classification ({@link WriteFailure#of}), plus: a producer that can no longer send
   * (closed by its owner) is {@code FATAL}.
   */
  @Override
  public WriteFailure classify(Throwable error) {
    return error instanceof ProducerUnusableException ? WriteFailure.FATAL : WriteFailure.of(error);
  }

  @Override
  public Optional<BackpressureSensor> sensor() {
    return Optional.ofNullable(sensor);
  }

  /**
   * Number of sends the producer has not completed yet.
   *
   * @return pending sends
   */
  public long pendingSends() {
    return pendingSends.get();
  }

  /** The producer, for a DLQ sink that shares it. */
  Producer<byte[], byte[]> producer() {
    return producer;
  }

  /**
   * Sends everything queued in the producer now instead of after {@code linger.ms}. The pipeline
   * calls this at shutdown, and after every write of a partition that is being revoked, so the
   * records it waits for are not held back by batching. One {@code producer.flush()} covers every
   * partition: requests made while a flush runs share the next flush, which starts after them, so
   * every send made before the request is covered. Never blocks the caller.
   */
  @Override
  public CompletionStage<Void> flush(TopicPartition partition) {
    return requestFlush();
  }

  /** Pushes out what the producer holds; the pipeline then waits for the partition's writes. */
  @Override
  public void onPartitionRevoked(TopicPartition partition, Duration budget) {
    requestFlush();
  }

  private CompletableFuture<Void> requestFlush() {
    CompletableFuture<Void> request = new CompletableFuture<>();
    boolean startFlusher;
    synchronized (flushRequests) {
      flushRequests.add(request);
      startFlusher = !flusherRunning;
      flusherRunning = true;
    }
    if (startFlusher) {
      // producer.flush() blocks until every queued send completes; never on the caller's thread.
      try {
        Thread.ofVirtual().name("kafka-sink-flush").start(this::flushUntilNoRequests);
      } catch (Throwable t) {
        List<CompletableFuture<Void>> failed;
        synchronized (flushRequests) {
          flusherRunning = false; // let the next request try again
          failed = new ArrayList<>(flushRequests);
          flushRequests.clear();
        }
        failed.forEach(f -> f.completeExceptionally(t));
        if (t instanceof Error e) throw e;
      }
    }
    return request;
  }

  private void flushUntilNoRequests() {
    while (true) {
      List<CompletableFuture<Void>> served;
      synchronized (flushRequests) {
        if (flushRequests.isEmpty()) {
          flusherRunning = false;
          return;
        }
        served = new ArrayList<>(flushRequests);
        flushRequests.clear();
      }
      // This flush starts after every request in `served` was made, so it covers their sends.
      try {
        producer.flush();
        served.forEach(f -> f.complete(null));
      } catch (Throwable t) {
        served.forEach(f -> f.completeExceptionally(t));
      }
    }
  }

  /**
   * Closes the serializers, like {@code KafkaProducer} closes the serializers it is given, and the
   * producer if this sink created it; a producer passed in is left to its owner.
   */
  @Override
  public void close() {
    try {
      closeSerializer(keySerializer);
      closeSerializer(valueSerializer);
    } finally {
      if (ownsProducer) producer.close(closeTimeout);
    }
  }

  static void closeSerializer(Serializer<?> serializer) {
    if (serializer == null) return;
    try {
      serializer.close();
    } catch (RuntimeException e) {
      System.getLogger(KafkaRecordSink.class.getName())
          .log(System.Logger.Level.WARNING, "Error closing serializer " + serializer, e);
    }
  }

  /**
   * Builder for {@link KafkaRecordSink}.
   *
   * @param <KO> output key type
   * @param <O> output type
   */
  public static final class Builder<KO, O> {
    private Properties producerProperties;
    private Producer<byte[], byte[]> producer;
    private Function<O, String> topicOf;
    private Function<O, KO> keyOf;
    private Serializer<KO> keySerializer;
    private Serializer<O> valueSerializer;
    private boolean sourceHeader = true;
    private long maxPendingSends = DEFAULT_MAX_PENDING_SENDS;
    private Duration closeTimeout = Duration.ofSeconds(30);

    private Builder() {}

    /**
     * Producer configuration; the sink creates the producer and closes it at shutdown. Serializer
     * settings are replaced, because the sink serializes with {@link #keySerializer} and {@link
     * #valueSerializer}. {@code acks} defaults to {@code all} and {@code enable.idempotence} to
     * {@code true}.
     *
     * @param props producer properties
     * @return this builder
     */
    public Builder<KO, O> producerProperties(Properties props) {
      Properties copy = new Properties();
      copy.putAll(Objects.requireNonNull(props, "props"));
      this.producerProperties = copy;
      return this;
    }

    /**
     * Uses an existing producer instead of creating one, for example to share it with other code.
     * The sink does not close it.
     *
     * @param producer a producer of serialized bytes
     * @return this builder
     */
    public Builder<KO, O> producer(Producer<byte[], byte[]> producer) {
      this.producer = Objects.requireNonNull(producer, "producer");
      return this;
    }

    /**
     * Writes every output to this topic.
     *
     * @param topic the topic
     * @return this builder
     */
    public Builder<KO, O> topic(String topic) {
      Objects.requireNonNull(topic, "topic");
      this.topicOf = output -> topic;
      return this;
    }

    /**
     * Chooses the topic per output.
     *
     * @param topicOf returns the topic for an output
     * @return this builder
     */
    public Builder<KO, O> topicFrom(Function<O, String> topicOf) {
      this.topicOf = Objects.requireNonNull(topicOf, "topicOf");
      return this;
    }

    /**
     * Derives the record key from the output. Without it, records are written without a key.
     *
     * @param keyOf returns the key for an output
     * @return this builder
     */
    public Builder<KO, O> keyFrom(Function<O, KO> keyOf) {
      this.keyOf = Objects.requireNonNull(keyOf, "keyOf");
      return this;
    }

    /**
     * Serializer for keys; required with {@link #keyFrom}.
     *
     * @param serializer the key serializer
     * @return this builder
     */
    public Builder<KO, O> keySerializer(Serializer<KO> serializer) {
      this.keySerializer = Objects.requireNonNull(serializer, "serializer");
      return this;
    }

    /**
     * Serializer for values (the outputs). Required.
     *
     * @param serializer the value serializer
     * @return this builder
     */
    public Builder<KO, O> valueSerializer(Serializer<O> serializer) {
      this.valueSerializer = Objects.requireNonNull(serializer, "serializer");
      return this;
    }

    /**
     * Whether each output carries the {@value KafkaHeaders#SOURCE} header with the input record's
     * id. Default: {@code true}.
     *
     * @param enabled whether to add the header
     * @return this builder
     */
    public Builder<KO, O> sourceHeader(boolean enabled) {
      this.sourceHeader = enabled;
      return this;
    }

    /**
     * Pending sends at which the pipeline stops fetching (global backpressure); it resumes at 60%
     * of this. Zero disables the sensor. Default: {@code 10,000}.
     *
     * @param max pending sends
     * @return this builder
     */
    public Builder<KO, O> maxPendingSends(long max) {
      if (max < 0) throw new IllegalArgumentException("maxPendingSends must be >= 0, got " + max);
      this.maxPendingSends = max;
      return this;
    }

    /**
     * How long closing a sink-owned producer may take at shutdown. Default: 30 seconds.
     *
     * @param timeout the close timeout
     * @return this builder
     */
    public Builder<KO, O> closeTimeout(Duration timeout) {
      this.closeTimeout = Objects.requireNonNull(timeout, "timeout");
      return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     * @throws IllegalStateException if a required setting is missing
     */
    public KafkaRecordSink<KO, O> build() {
      if (producer == null && producerProperties == null) {
        throw new IllegalStateException("Set producerProperties(...) or producer(...)");
      }
      if (producer != null && producerProperties != null) {
        throw new IllegalStateException("Set producerProperties(...) or producer(...), not both");
      }
      if (topicOf == null) throw new IllegalStateException("Set topic(...) or topicFrom(...)");
      if (valueSerializer == null) throw new IllegalStateException("Set valueSerializer(...)");
      if (keyOf != null && keySerializer == null) {
        throw new IllegalStateException("keyFrom(...) needs keySerializer(...)");
      }
      return new KafkaRecordSink<>(this);
    }
  }
}
