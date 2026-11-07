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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.ByteBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.HeapBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.handler.BatchRecordHandler;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * Top-level configuration for a {@link KafkaPipeline}.
 *
 * @param <K> record key type
 * @param <V> record value type
 * @param consumerProperties Kafka consumer configuration
 * @param topics topics to subscribe to
 * @param handler per-record handler (mutually exclusive with batchHandler)
 * @param batchHandler batch handler (mutually exclusive with handler)
 * @param concurrency number of worker threads
 * @param threadMode platform or virtual threads
 * @param backpressure record-count backpressure configuration
 * @param byteBackpressure byte-level backpressure configuration
 * @param heapBackpressure heap-based backpressure configuration
 * @param customSensors custom backpressure sensors
 * @param errorStrategy retry and error handling configuration
 * @param lifecycleHook processing lifecycle callbacks
 * @param commitInterval interval between async offset commits
 * @param drainTimeout max wait for in-flight records during rebalance
 * @param shutdownTimeout max wait for workers during shutdown
 * @param pollTimeout Kafka consumer poll timeout
 * @param dispatchQueueCapacity capacity of per-partition dispatch queues
 * @param maxWindowSize max tracked offsets per partition before that partition is paused
 */
public record PipelineConfig<K, V>(
    Properties consumerProperties,
    List<String> topics,
    RecordHandler<K, V> handler,
    BatchRecordHandler<K, V> batchHandler,
    int concurrency,
    ThreadMode threadMode,
    BackpressureConfig backpressure,
    ByteBackpressureConfig byteBackpressure,
    HeapBackpressureConfig heapBackpressure,
    List<BackpressureSensor> customSensors,
    ErrorStrategy<K, V> errorStrategy,
    ProcessingLifecycleHook<K, V> lifecycleHook,
    Duration commitInterval,
    Duration drainTimeout,
    Duration shutdownTimeout,
    Duration pollTimeout,
    int dispatchQueueCapacity,
    int maxWindowSize) {
  /** Validates configuration constraints. */
  public PipelineConfig {
    Objects.requireNonNull(consumerProperties, "consumerProperties");
    Objects.requireNonNull(topics, "topics");
    Objects.requireNonNull(threadMode, "threadMode");
    Objects.requireNonNull(backpressure, "backpressure");
    Objects.requireNonNull(byteBackpressure, "byteBackpressure");
    Objects.requireNonNull(heapBackpressure, "heapBackpressure");
    Objects.requireNonNull(customSensors, "customSensors");
    Objects.requireNonNull(errorStrategy, "errorStrategy");
    Objects.requireNonNull(lifecycleHook, "lifecycleHook");

    if (handler == null && batchHandler == null) {
      throw new IllegalArgumentException("Either handler or batchHandler must be set");
    }
    if (handler != null && batchHandler != null) {
      throw new IllegalArgumentException("Cannot set both handler and batchHandler");
    }
    if (topics.isEmpty()) {
      throw new IllegalArgumentException("topics must not be empty");
    }
    if (concurrency <= 0) {
      throw new IllegalArgumentException("concurrency must be positive, got " + concurrency);
    }
    if (dispatchQueueCapacity <= 0) {
      throw new IllegalArgumentException("dispatchQueueCapacity must be positive");
    }
    if (maxWindowSize <= 0) {
      throw new IllegalArgumentException("maxWindowSize must be positive, got " + maxWindowSize);
    }
    int maxPollRecords = maxPollRecords(consumerProperties);
    if (maxWindowSize < maxPollRecords) {
      throw new IllegalArgumentException(
          "maxWindowSize ("
              + maxWindowSize
              + ") must be >= max.poll.records ("
              + maxPollRecords
              + "), otherwise a single poll may never fit in a partition window");
    }
  }

  private static int maxPollRecords(Properties props) {
    Object value = props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    if (value == null) {
      return ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    }
    try {
      return Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid max.poll.records: " + value, e);
    }
  }

  boolean isBatchMode() {
    return batchHandler != null;
  }

  /**
   * Creates a new builder.
   *
   * @param <K> record key type
   * @param <V> record value type
   * @return new builder instance
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /**
   * Builder for {@link PipelineConfig}.
   *
   * @param <K> record key type
   * @param <V> record value type
   */
  public static final class Builder<K, V> {
    private Properties consumerProperties;
    private List<String> topics;
    private RecordHandler<K, V> handler;
    private BatchRecordHandler<K, V> batchHandler;
    private int concurrency = -1;
    private ThreadMode threadMode;
    private BackpressureConfig backpressure = BackpressureConfig.defaults();
    private ByteBackpressureConfig byteBackpressure = ByteBackpressureConfig.disabled();
    private HeapBackpressureConfig heapBackpressure = HeapBackpressureConfig.disabled();
    private List<BackpressureSensor> customSensors = new ArrayList<>();
    private ErrorStrategy<K, V> errorStrategy = ErrorStrategy.skipOnError();
    private ProcessingLifecycleHook<K, V> lifecycleHook = ProcessingLifecycleHook.noOp();
    private Duration commitInterval = Duration.ofSeconds(5);
    private Duration drainTimeout = Duration.ofSeconds(30);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration pollTimeout = Duration.ofMillis(100);
    private int dispatchQueueCapacity = 500;
    private int maxWindowSize = 10_000;

    private Builder() {}

    /**
     * Standard Kafka consumer properties ({@code bootstrap.servers}, {@code group.id},
     * deserializers, etc.). {@code enable.auto.commit} is forced to {@code false} internally.
     *
     * @param props the consumer properties
     * @return this builder
     */
    public Builder<K, V> consumerProperties(Properties props) {
      this.consumerProperties = props;
      return this;
    }

    /**
     * The topic to subscribe to. This is the recommended way to configure a pipeline: one topic per
     * pipeline, so each topic gets its own thread pool, backpressure and consumer group.
     *
     * @param topic the topic
     * @return this builder
     * @see #topics(String...)
     */
    public Builder<K, V> topic(String topic) {
      this.topics = List.of(Objects.requireNonNull(topic, "topic"));
      return this;
    }

    /**
     * Subscribes one pipeline to several topics. Prefer {@link #topic(String)} with one pipeline
     * per topic.
     *
     * <p>Topics in the same pipeline share the worker thread pool, the global backpressure
     * watermarks and the consumer: a slow topic can occupy every worker, and backpressure caused by
     * one topic pauses all of them. Only use this for topics that are the same workload (for
     * example regional shards such as {@code orders-us} and {@code orders-eu}). Run a separate
     * pipeline, with its own {@code group.id}, for topics that need isolation.
     *
     * @param topics the topics
     * @return this builder
     */
    public Builder<K, V> topics(List<String> topics) {
      this.topics = topics;
      return this;
    }

    /**
     * Subscribes one pipeline to several topics. Prefer {@link #topic(String)}; see {@link
     * #topics(List)} for what topics in one pipeline share.
     *
     * @param topics the topics
     * @return this builder
     */
    public Builder<K, V> topics(String... topics) {
      this.topics = List.of(topics);
      return this;
    }

    /**
     * Per-record handler. Mutually exclusive with {@link #batchHandler}; exactly one must be set.
     *
     * @param handler the record handler
     * @return this builder
     */
    public Builder<K, V> handler(RecordHandler<K, V> handler) {
      this.handler = handler;
      return this;
    }

    /**
     * Batch handler for high-throughput scenarios (bulk inserts, aggregations). Records from each
     * partition in a single poll are delivered as one batch. Mutually exclusive with {@link
     * #handler}; exactly one must be set.
     *
     * @param batchHandler the batch handler
     * @return this builder
     */
    public Builder<K, V> batchHandler(BatchRecordHandler<K, V> batchHandler) {
      this.batchHandler = batchHandler;
      return this;
    }

    /**
     * Thread model and worker count. This is required — the library does not default concurrency to
     * avoid silent surprises in production.
     *
     * <ul>
     *   <li>{@link ThreadMode#PLATFORM} — fixed pool of OS threads, good for CPU-bound work
     *   <li>{@link ThreadMode#VIRTUAL} — JDK 21+ virtual threads, good for I/O-bound work (DB
     *       calls, HTTP, etc.)
     * </ul>
     *
     * @param threadMode platform or virtual threads
     * @param concurrency number of worker threads (must be positive)
     * @return this builder
     */
    public Builder<K, V> concurrency(ThreadMode threadMode, int concurrency) {
      this.threadMode = threadMode;
      this.concurrency = concurrency;
      return this;
    }

    /**
     * Record-count backpressure thresholds. Enabled by default with sensible defaults (high=10k,
     * low=6k, critical=50k). The poll loop pauses when in-flight record count crosses the high
     * watermark and resumes when it drops below the low watermark.
     *
     * @param backpressure the backpressure configuration
     * @return this builder
     * @see BackpressureConfig#defaults()
     * @see BackpressureConfig#builder()
     */
    public Builder<K, V> backpressure(BackpressureConfig backpressure) {
      this.backpressure = backpressure;
      return this;
    }

    /**
     * Byte-level backpressure thresholds. Disabled by default. Protects against memory pressure
     * when individual records are large (e.g. 10 MB payloads). Tracks total in-flight bytes using
     * {@code ConsumerRecord.serializedKeySize() + serializedValueSize()}.
     *
     * @param byteBackpressure the byte backpressure configuration
     * @return this builder
     * @see ByteBackpressureConfig#builder()
     */
    public Builder<K, V> byteBackpressure(ByteBackpressureConfig byteBackpressure) {
      this.byteBackpressure = byteBackpressure;
      return this;
    }

    /**
     * Heap-based backpressure thresholds. Disabled by default. Protects against OOM by monitoring
     * JVM heap usage via {@code MemoryMXBean}. Works regardless of pressure source — in-flight
     * records, producer buffers, caches, or any other heap-resident data.
     *
     * @param heapBackpressure the heap backpressure configuration
     * @return this builder
     * @see HeapBackpressureConfig#builder()
     */
    public Builder<K, V> heapBackpressure(HeapBackpressureConfig heapBackpressure) {
      this.heapBackpressure = heapBackpressure;
      return this;
    }

    /**
     * Registers a custom backpressure sensor. Multiple sensors can be added; the pipeline evaluates
     * all of them (built-in and custom) on each poll cycle and applies the worst status.
     *
     * @param sensor the custom sensor to add
     * @return this builder
     * @see BackpressureSensor
     */
    public Builder<K, V> addBackpressureSensor(BackpressureSensor sensor) {
      this.customSensors.add(Objects.requireNonNull(sensor, "sensor"));
      return this;
    }

    /**
     * Error handling chain applied when {@code handler.handle()} or {@code batchHandler} throws.
     * The chain is: retry → DLQ → skip. Defaults to {@link ErrorStrategy#skipOnError()} (no
     * retries, skip failed records with logging).
     *
     * @param errorStrategy the error strategy
     * @return this builder
     * @see ErrorStrategy#builder()
     * @see ErrorStrategy#withDlq(io.github.openlogiclab.kafkapipeline.error.DLQHandler, int)
     */
    public Builder<K, V> errorStrategy(ErrorStrategy<K, V> errorStrategy) {
      this.errorStrategy = errorStrategy;
      return this;
    }

    /**
     * Per-record lifecycle callbacks for cross-cutting concerns (dedup, metrics, tracing). Only
     * applies in per-record mode ({@link #handler}); batch mode does not invoke hooks. Defaults to
     * no-op.
     *
     * @param hook the lifecycle hook
     * @return this builder
     */
    public Builder<K, V> lifecycleHook(ProcessingLifecycleHook<K, V> hook) {
      this.lifecycleHook = hook;
      return this;
    }

    /**
     * How often to commit offsets asynchronously. A final synchronous commit is always performed on
     * shutdown and rebalance regardless of this interval.
     *
     * <p>Default: {@code 5 seconds}.
     *
     * @param interval the commit interval
     * @return this builder
     */
    public Builder<K, V> commitInterval(Duration interval) {
      this.commitInterval = interval;
      return this;
    }

    /**
     * Maximum time to wait for in-flight records to complete during a rebalance before abandoning
     * them. Records still in-flight after this timeout are abandoned (not committed), and will be
     * redelivered to the new partition owner.
     *
     * <p>Default: {@code 30 seconds}.
     *
     * @param timeout the drain timeout
     * @return this builder
     */
    public Builder<K, V> drainTimeout(Duration timeout) {
      this.drainTimeout = timeout;
      return this;
    }

    /**
     * Maximum time to wait for worker threads to finish during {@code stop()}.
     *
     * <p>Default: {@code 30 seconds}.
     *
     * @param timeout the shutdown timeout
     * @return this builder
     */
    public Builder<K, V> shutdownTimeout(Duration timeout) {
      this.shutdownTimeout = timeout;
      return this;
    }

    /**
     * Kafka consumer poll timeout. Lower values reduce latency; higher values reduce CPU usage when
     * idle.
     *
     * <p>Default: {@code 100ms}.
     *
     * @param timeout the poll timeout
     * @return this builder
     */
    public Builder<K, V> pollTimeout(Duration timeout) {
      this.pollTimeout = timeout;
      return this;
    }

    /**
     * Bounded queue capacity used for buffering pending work.
     *
     * <ul>
     *   <li>In <b>per-record mode</b>: capacity of each per-partition dispatch queue.
     *   <li>In <b>batch mode</b>: capacity of the executor's task queue (PLATFORM threads only;
     *       virtual threads are unbounded by nature).
     * </ul>
     *
     * <p>A good starting point is Kafka's {@code max.poll.records} (default 500). If the queue
     * fills up, the dispatch/submit call blocks the poll thread (with a WARNING log) until space is
     * available — this acts as a safety net but should not happen under normal operation when
     * backpressure thresholds are configured correctly.
     *
     * <p><b>Warning:</b> setting this too high wastes memory and reduces backpressure sensitivity
     * (records sit in the queue longer before backpressure reacts). Setting it too low causes
     * frequent blocking of the poll thread, which degrades throughput.
     *
     * <p>Default: {@code 500} (matches Kafka's default {@code max.poll.records}).
     *
     * @param capacity the queue capacity
     * @return this builder
     */
    public Builder<K, V> dispatchQueueCapacity(int capacity) {
      this.dispatchQueueCapacity = capacity;
      return this;
    }

    /**
     * Maximum number of offsets tracked per partition before that partition is paused.
     *
     * <p>The window holds every offset from the oldest unfinished record up to the newest fetched
     * one, including records that already completed behind it. When a partition's window reaches
     * 80% of this size, the consumer pauses that partition only (other partitions keep flowing) and
     * resumes it once the window drops to 50%. A larger window gives slow records more slack but
     * uses more memory (roughly 50 bytes per entry) and means more records are redelivered after a
     * crash.
     *
     * <p>Must be at least {@code max.poll.records}.
     *
     * <p>Default: {@code 10000}.
     *
     * @param maxWindowSize the per-partition window capacity
     * @return this builder
     */
    public Builder<K, V> maxWindowSize(int maxWindowSize) {
      this.maxWindowSize = maxWindowSize;
      return this;
    }

    /**
     * Builds an immutable {@link PipelineConfig}.
     *
     * @return the built configuration
     * @throws IllegalStateException if concurrency is not configured
     * @throws IllegalArgumentException if validation fails (missing handler, empty topics, etc.)
     */
    public PipelineConfig<K, V> build() {
      if (threadMode == null) {
        throw new IllegalStateException(
            "Concurrency not configured. Call concurrency(ThreadMode, int) to set thread model and"
                + " worker count");
      }
      return new PipelineConfig<>(
          consumerProperties,
          topics,
          handler,
          batchHandler,
          concurrency,
          threadMode,
          backpressure,
          byteBackpressure,
          heapBackpressure,
          List.copyOf(customSensors),
          errorStrategy,
          lifecycleHook,
          commitInterval,
          drainTimeout,
          shutdownTimeout,
          pollTimeout,
          dispatchQueueCapacity,
          maxWindowSize);
    }
  }
}
