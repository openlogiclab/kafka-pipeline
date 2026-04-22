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

/**
 * Top-level configuration for a {@link KafkaPipeline}.
 *
 * @param <K> record key type
 * @param <V> record value type
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
    int dispatchQueueCapacity) {
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
  }

  boolean isBatchMode() {
    return batchHandler != null;
  }

  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /** Builder for {@link PipelineConfig}. */
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
    private ErrorStrategy<K, V> errorStrategy = ErrorStrategy.failFast();
    private ProcessingLifecycleHook<K, V> lifecycleHook = ProcessingLifecycleHook.noOp();
    private Duration commitInterval = Duration.ofSeconds(5);
    private Duration drainTimeout = Duration.ofSeconds(30);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration pollTimeout = Duration.ofMillis(100);
    private int dispatchQueueCapacity = 500;

    private Builder() {}

    /**
     * Standard Kafka consumer properties ({@code bootstrap.servers}, {@code group.id},
     * deserializers, etc.). {@code enable.auto.commit} is forced to {@code false} internally.
     */
    public Builder<K, V> consumerProperties(Properties props) {
      this.consumerProperties = props;
      return this;
    }

    /** Topics to subscribe to. At least one is required. */
    public Builder<K, V> topics(List<String> topics) {
      this.topics = topics;
      return this;
    }

    /** Topics to subscribe to. At least one is required. */
    public Builder<K, V> topics(String... topics) {
      this.topics = List.of(topics);
      return this;
    }

    /**
     * Per-record handler. Mutually exclusive with {@link #batchHandler}; exactly one must be set.
     */
    public Builder<K, V> handler(RecordHandler<K, V> handler) {
      this.handler = handler;
      return this;
    }

    /**
     * Batch handler for high-throughput scenarios (bulk inserts, aggregations). Records from each
     * partition in a single poll are delivered as one batch. Mutually exclusive with {@link
     * #handler}; exactly one must be set.
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
     * @see BackpressureSensor
     */
    public Builder<K, V> addBackpressureSensor(BackpressureSensor sensor) {
      this.customSensors.add(Objects.requireNonNull(sensor, "sensor"));
      return this;
    }

    /**
     * Error handling chain applied when {@code handler.handle()} or {@code batchHandler} throws.
     * The chain is: retry → DLQ → fallback. Defaults to {@link ErrorStrategy#failFast()} (no
     * retries, halt partition on error).
     *
     * @see ErrorStrategy#builder()
     * @see ErrorStrategy#skipOnError()
     */
    public Builder<K, V> errorStrategy(ErrorStrategy<K, V> errorStrategy) {
      this.errorStrategy = errorStrategy;
      return this;
    }

    /**
     * Per-record lifecycle callbacks for cross-cutting concerns (dedup, metrics, tracing). Only
     * applies in per-record mode ({@link #handler}); batch mode does not invoke hooks. Defaults to
     * no-op.
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
     */
    public Builder<K, V> drainTimeout(Duration timeout) {
      this.drainTimeout = timeout;
      return this;
    }

    /**
     * Maximum time to wait for worker threads to finish during {@code stop()}.
     *
     * <p>Default: {@code 30 seconds}.
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
     */
    public Builder<K, V> dispatchQueueCapacity(int capacity) {
      this.dispatchQueueCapacity = capacity;
      return this;
    }

    /**
     * Builds an immutable {@link PipelineConfig}.
     *
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
          dispatchQueueCapacity);
    }
  }
}
