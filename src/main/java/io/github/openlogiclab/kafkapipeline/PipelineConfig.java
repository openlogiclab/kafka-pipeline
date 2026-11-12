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
import io.github.openlogiclab.kafkapipeline.metrics.MetricsExporter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * Top-level configuration for a {@link KafkaPipeline}. Created only through {@link #builder()}, so
 * new options can be added without breaking callers.
 *
 * <p>Instances are immutable.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class PipelineConfig<K, V> {
  /** Default name used when neither {@code name} nor {@code group.id} is set. */
  public static final String DEFAULT_NAME = "kafka-pipeline";

  private final Properties consumerProperties;
  private final List<String> topics;
  private final RecordHandler<K, V> handler;
  private final BatchRecordHandler<K, V> batchHandler;
  private final int concurrency;
  private final ThreadMode threadMode;
  private final BackpressureConfig backpressure;
  private final ByteBackpressureConfig byteBackpressure;
  private final HeapBackpressureConfig heapBackpressure;
  private final List<BackpressureSensor> customSensors;
  private final ErrorStrategy<K, V> errorStrategy;
  private final ProcessingLifecycleHook<K, V> lifecycleHook;
  private final Duration commitInterval;
  private final Duration drainTimeout;
  private final Duration shutdownTimeout;
  private final Duration pollTimeout;
  private final int dispatchQueueCapacity;
  private final int maxWindowSize;
  private final int maxConcurrencyPerPartition;
  private final ProcessingOrder ordering;
  private final boolean skipCompletedOnRestart;
  private final Duration processingTimeout;
  private final String name;
  private final List<MetricsExporter> metricsExporters;

  /** Validates the builder's values and resolves the default name. */
  private PipelineConfig(Builder<K, V> b) {
    this.consumerProperties = copyOf(b.consumerProperties);
    this.topics = b.topics == null ? null : List.copyOf(b.topics);
    this.handler = b.handler;
    this.batchHandler = b.batchHandler;
    this.concurrency = b.concurrency;
    this.threadMode = b.threadMode;
    this.backpressure = b.backpressure;
    this.byteBackpressure = b.byteBackpressure;
    this.heapBackpressure = b.heapBackpressure;
    this.customSensors = List.copyOf(b.customSensors);
    this.errorStrategy = b.errorStrategy;
    this.lifecycleHook = b.lifecycleHook;
    this.commitInterval = b.commitInterval;
    this.drainTimeout = b.drainTimeout;
    this.shutdownTimeout = b.shutdownTimeout;
    this.pollTimeout = b.pollTimeout;
    this.dispatchQueueCapacity = b.dispatchQueueCapacity;
    this.maxWindowSize = b.maxWindowSize;
    this.maxConcurrencyPerPartition = b.maxConcurrencyPerPartition;
    this.ordering = b.ordering;
    this.skipCompletedOnRestart = b.skipCompletedOnRestart;
    this.processingTimeout = b.processingTimeout;
    this.metricsExporters = List.copyOf(b.metricsExporters);
    Objects.requireNonNull(consumerProperties, "consumerProperties");
    Objects.requireNonNull(topics, "topics");
    Objects.requireNonNull(threadMode, "threadMode");
    Objects.requireNonNull(backpressure, "backpressure");
    Objects.requireNonNull(byteBackpressure, "byteBackpressure");
    Objects.requireNonNull(heapBackpressure, "heapBackpressure");
    Objects.requireNonNull(errorStrategy, "errorStrategy");
    Objects.requireNonNull(lifecycleHook, "lifecycleHook");
    Objects.requireNonNull(ordering, "ordering");
    requirePositive(commitInterval, "commitInterval");
    requireNonNegative(drainTimeout, "drainTimeout");
    requireNonNegative(shutdownTimeout, "shutdownTimeout");
    requireNonNegative(pollTimeout, "pollTimeout");
    requireNonNegative(processingTimeout, "processingTimeout");

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
    if (maxConcurrencyPerPartition < 0) {
      throw new IllegalArgumentException(
          "maxConcurrencyPerPartition must be >= 0, got " + maxConcurrencyPerPartition);
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
    long maxPollIntervalMs = maxPollIntervalMs(consumerProperties);
    if (drainTimeout.toMillis() >= maxPollIntervalMs) {
      // The revoke callback blocks the poll thread for up to drainTimeout; reaching
      // max.poll.interval.ms there makes the consumer leave the group mid-rebalance.
      throw new IllegalArgumentException(
          "drainTimeout ("
              + drainTimeout
              + ") must be less than max.poll.interval.ms ("
              + maxPollIntervalMs
              + "ms)");
    }
    this.name = resolveName(b.name, consumerProperties);
  }

  /**
   * Kafka consumer configuration. Returns a copy; changing it does not change this config.
   *
   * @return a copy of the consumer properties
   */
  public Properties consumerProperties() {
    return copyOf(consumerProperties);
  }

  /**
   * Returns the topics to subscribe to.
   *
   * @return the topics
   */
  public List<String> topics() {
    return topics;
  }

  /**
   * Returns the per-record handler (mutually exclusive with batchHandler).
   *
   * @return the handler
   */
  public RecordHandler<K, V> handler() {
    return handler;
  }

  /**
   * Returns the batch handler (mutually exclusive with handler).
   *
   * @return the batchHandler
   */
  public BatchRecordHandler<K, V> batchHandler() {
    return batchHandler;
  }

  /**
   * Returns the number of worker threads.
   *
   * @return the concurrency
   */
  public int concurrency() {
    return concurrency;
  }

  /**
   * Returns whether workers run on platform or virtual threads.
   *
   * @return the threadMode
   */
  public ThreadMode threadMode() {
    return threadMode;
  }

  /**
   * Returns the record-count backpressure configuration.
   *
   * @return the backpressure
   */
  public BackpressureConfig backpressure() {
    return backpressure;
  }

  /**
   * Returns the byte-level backpressure configuration.
   *
   * @return the byteBackpressure
   */
  public ByteBackpressureConfig byteBackpressure() {
    return byteBackpressure;
  }

  /**
   * Returns the heap-based backpressure configuration.
   *
   * @return the heapBackpressure
   */
  public HeapBackpressureConfig heapBackpressure() {
    return heapBackpressure;
  }

  /**
   * Returns the custom backpressure sensors.
   *
   * @return the customSensors
   */
  public List<BackpressureSensor> customSensors() {
    return customSensors;
  }

  /**
   * Returns the retry and error handling configuration.
   *
   * @return the errorStrategy
   */
  public ErrorStrategy<K, V> errorStrategy() {
    return errorStrategy;
  }

  /**
   * Returns the processing lifecycle callbacks.
   *
   * @return the lifecycleHook
   */
  public ProcessingLifecycleHook<K, V> lifecycleHook() {
    return lifecycleHook;
  }

  /**
   * Returns the interval between async offset commits.
   *
   * @return the commitInterval
   */
  public Duration commitInterval() {
    return commitInterval;
  }

  /**
   * Returns the max wait for in-flight records during rebalance.
   *
   * @return the drainTimeout
   */
  public Duration drainTimeout() {
    return drainTimeout;
  }

  /**
   * Returns the max wait for workers during shutdown.
   *
   * @return the shutdownTimeout
   */
  public Duration shutdownTimeout() {
    return shutdownTimeout;
  }

  /**
   * Returns the Kafka consumer poll timeout.
   *
   * @return the pollTimeout
   */
  public Duration pollTimeout() {
    return pollTimeout;
  }

  /**
   * Returns the capacity of per-partition dispatch queues.
   *
   * @return the dispatchQueueCapacity
   */
  public int dispatchQueueCapacity() {
    return dispatchQueueCapacity;
  }

  /**
   * Returns the max tracked offsets per partition before that partition is paused.
   *
   * @return the maxWindowSize
   */
  public int maxWindowSize() {
    return maxWindowSize;
  }

  /**
   * Returns the max records (per-record mode) or batches (batch mode) of one partition processed at
   * once; 0 means unlimited.
   *
   * @return the maxConcurrencyPerPartition
   */
  public int maxConcurrencyPerPartition() {
    return maxConcurrencyPerPartition;
  }

  /**
   * Returns the ordering guarantee for records of the same partition.
   *
   * @return the ordering
   */
  public ProcessingOrder ordering() {
    return ordering;
  }

  /**
   * Returns whether finished-but-uncommitted offsets are recorded in commit metadata and skipped
   * when the partition is consumed again.
   *
   * @return the skipCompletedOnRestart
   */
  public boolean skipCompletedOnRestart() {
    return skipCompletedOnRestart;
  }

  /**
   * Returns the max duration of one handler attempt before it is interrupted and counted as failed;
   * zero disables it.
   *
   * @return the processingTimeout
   */
  public Duration processingTimeout() {
    return processingTimeout;
  }

  /**
   * Returns the name that identifies this pipeline in metrics. Defaults to the consumer's {@code
   * group.id}, or {@value #DEFAULT_NAME}.
   *
   * @return the name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the exporters that publish this pipeline's metrics, in registration order.
   *
   * @return the metrics exporters; empty if none
   */
  public List<MetricsExporter> metricsExporters() {
    return metricsExporters;
  }

  /**
   * Describes this configuration. Consumer properties are left out because they may hold
   * credentials such as {@code sasl.jaas.config}.
   */
  @Override
  public String toString() {
    return "PipelineConfig[name="
        + name
        + ", topics="
        + topics
        + ", mode="
        + (isBatchMode() ? "batch" : "record")
        + ", concurrency="
        + concurrency
        + ", threadMode="
        + threadMode
        + ", ordering="
        + ordering
        + ", maxWindowSize="
        + maxWindowSize
        + ", maxConcurrencyPerPartition="
        + maxConcurrencyPerPartition
        + ", commitInterval="
        + commitInterval
        + ", processingTimeout="
        + processingTimeout
        + "]";
  }

  private static Properties copyOf(Properties props) {
    if (props == null) return null;
    Properties copy = new Properties();
    copy.putAll(props);
    return copy;
  }

  private static String resolveName(String name, Properties props) {
    if (name != null) {
      if (name.isBlank()) {
        throw new IllegalArgumentException("name must not be blank");
      }
      return name;
    }
    Object groupId = props.get(ConsumerConfig.GROUP_ID_CONFIG);
    return groupId != null && !groupId.toString().isBlank() ? groupId.toString() : DEFAULT_NAME;
  }

  private static void requirePositive(Duration d, String name) {
    Objects.requireNonNull(d, name);
    if (d.isZero() || d.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive, got " + d);
    }
  }

  private static void requireNonNegative(Duration d, String name) {
    Objects.requireNonNull(d, name);
    if (d.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative, got " + d);
    }
  }

  private static long maxPollIntervalMs(Properties props) {
    Object value = props.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    if (value == null) {
      return 300_000L; // Kafka's default max.poll.interval.ms
    }
    try {
      return Long.parseLong(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid max.poll.interval.ms: " + value, e);
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
    private int maxConcurrencyPerPartition = 0;
    private ProcessingOrder ordering = ProcessingOrder.UNORDERED;
    private boolean skipCompletedOnRestart = true;
    private Duration processingTimeout = Duration.ZERO;
    private int maxWindowSize = 10_000;
    private String name;
    private final List<MetricsExporter> metricsExporters = new ArrayList<>();

    private Builder() {}

    /**
     * Adds an exporter that publishes this pipeline's metrics, for example to Micrometer or
     * OpenTelemetry. Call it once per exporter; they are bound in the order added.
     *
     * @param exporter the exporter
     * @return this builder
     * @see MetricsExporter
     */
    public Builder<K, V> metricsExporter(MetricsExporter exporter) {
      metricsExporters.add(Objects.requireNonNull(exporter, "exporter"));
      return this;
    }

    /**
     * Name of this pipeline, used to tell pipelines apart in metrics (for example as a tag).
     *
     * <p>Default: the consumer's {@code group.id}, or {@value PipelineConfig#DEFAULT_NAME} if that
     * is not set either.
     *
     * @param name the pipeline name; must not be blank
     * @return this builder
     */
    public Builder<K, V> name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      return this;
    }

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
     * them. Must be less than the consumer's {@code max.poll.interval.ms}, because the poll thread
     * waits for the drain. Records still in-flight after this timeout are abandoned (not
     * committed), and will be redelivered to the new partition owner.
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
     * <p>The same timeout bounds the consumer close afterwards, so a full shutdown can take up to
     * about twice this value. Keep that below the time your platform allows before a hard kill (for
     * example Kubernetes' {@code terminationGracePeriodSeconds}, 30 seconds by default), or the
     * final commit is lost and finished records are processed again after restart.
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
     * Maximum duration of one handler attempt. An attempt still running after this is interrupted
     * and counted as a failed attempt, so it goes through the error strategy (retry, then DLQ or
     * skip) and its partition can commit past it. Without a timeout, a handler call that never
     * returns holds back its partition's commits until a rebalance.
     *
     * <p>The timeout works by interrupting the worker thread: a handler that ignores interrupts
     * (for example blocking socket I/O without its own timeout) cannot be stopped this way and
     * keeps its worker until it returns. Give I/O clients their own timeouts as well.
     *
     * <p>Default: {@code 0} (disabled).
     *
     * @param timeout the per-attempt timeout; zero disables it
     * @return this builder
     */
    public Builder<K, V> processingTimeout(Duration timeout) {
      this.processingTimeout = timeout;
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
     * fills up, the poll thread never blocks: that partition is paused, the records that did not
     * fit are fetched again later (seek back to the first one), and a rate-limited WARNING is
     * logged. The partition resumes once its queue has drained to half capacity. This is a safety
     * net and should be rare when backpressure thresholds are configured correctly.
     *
     * <p><b>Warning:</b> setting this too high wastes memory and reduces backpressure sensitivity
     * (records sit in the queue longer before backpressure reacts). Setting it too low causes
     * frequent pause/refetch cycles, which waste fetch bandwidth and degrade throughput.
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
     * Maximum number of records (per-record mode) or batches (batch mode) from the same partition
     * that may be processed at the same time.
     *
     * <p>All partitions share one worker pool. Without a limit, a partition whose records are slow
     * (a slow downstream, a hot key range) can occupy every worker while records of other
     * partitions wait in their queues. With a limit of {@code n}, at most {@code n} workers work on
     * one partition; the rest stay available to the others.
     *
     * <p>The limit never drops below a fair share of the pool: the enforced value is {@code
     * max(max, ceil(concurrency / assignedPartitions))}, recomputed on every rebalance. So with a
     * single assigned partition all workers can still be used, and the configured value only bites
     * once there are enough partitions to share the pool. A good starting point is about {@code 2 ×
     * concurrency / expectedPartitionsPerInstance}.
     *
     * <p>In unordered batch mode the limit counts batches of one partition in flight. Without it, a
     * slow partition keeps submitting batches until its window is nearly full, and those records
     * alone can push the shared in-flight count over the backpressure high watermark and throttle
     * every partition. A partition at its limit is paused on its own until a batch finishes.
     * Ordered batch modes always allow one batch per partition.
     *
     * <p>Default: {@code 0} (unlimited).
     *
     * @param max the per-partition limit, or 0 for no limit
     * @return this builder
     */
    public Builder<K, V> maxConcurrencyPerPartition(int max) {
      this.maxConcurrencyPerPartition = max;
      return this;
    }

    /**
     * Ordering guarantee for records of the same partition.
     *
     * <ul>
     *   <li>{@link ProcessingOrder#UNORDERED} (default): maximum parallelism, records may finish in
     *       any order.
     *   <li>{@link ProcessingOrder#KEY}: records with the same key run one at a time, in offset
     *       order; different keys run in parallel. Records with a null key are unordered.
     *   <li>{@link ProcessingOrder#PARTITION}: records of a partition run strictly one at a time.
     * </ul>
     *
     * <p>In batch mode, {@code KEY} and {@code PARTITION} both mean at most one batch per partition
     * in flight.
     *
     * @param ordering the ordering guarantee
     * @return this builder
     */
    public Builder<K, V> ordering(ProcessingOrder ordering) {
      this.ordering = ordering;
      return this;
    }

    /**
     * Whether to skip records that already finished before a crash or rebalance.
     *
     * <p>Records complete out of order, but Kafka stores one committed offset per partition: the
     * oldest record not yet done. Without this, every record after it is processed again when the
     * partition is consumed again, including ones that had already finished (up to {@link
     * #maxWindowSize} per partition). With it, each commit also stores which later offsets are
     * done, as a compressed bitmap in the commit's metadata, and those records are skipped on
     * restart. If the bitmap does not fit Kafka's metadata limit, the commit is made without it.
     *
     * <p>Requires the broker's {@code offset.metadata.max.bytes} to be at least 4096 (the default).
     * Turn it off if something else writes metadata on this consumer group's commits.
     *
     * <p>Default: {@code true}.
     *
     * @param skip whether to record and skip completed offsets
     * @return this builder
     */
    public Builder<K, V> skipCompletedOnRestart(boolean skip) {
      this.skipCompletedOnRestart = skip;
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
      return new PipelineConfig<>(this);
    }
  }
}
