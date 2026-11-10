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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureController;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.ByteSizeSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.HeapSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.backpressure.RecordCountSensor;
import io.github.openlogiclab.kafkapipeline.dispatch.PartitionPauseController;
import io.github.openlogiclab.kafkapipeline.dispatch.PeriodicCommitter;
import io.github.openlogiclab.kafkapipeline.dispatch.PipelineRebalanceListener;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import io.github.openlogiclab.kafkapipeline.worker.BatchWorkerPool;
import io.github.openlogiclab.kafkapipeline.worker.RetryExecutor;
import io.github.openlogiclab.kafkapipeline.worker.SingleRecordWorkerPool;
import io.github.openlogiclab.kafkapipeline.worker.WorkerPool;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Main entry point for the Kafka Pipeline library. Wraps a {@link KafkaConsumer} with concurrent
 * processing (unordered, or ordered per key or partition), backpressure, configurable error
 * handling (retry → DLQ → fallback), lifecycle hooks, and automatic offset management — so users
 * only need to focus on processing each record.
 *
 * <h2>Architecture</h2>
 *
 * <pre>
 *   KafkaConsumer.poll()              (single-threaded poll loop)
 *         │
 *         ▼
 *   RecordDispatcher                  (per-partition queues)
 *         │
 *   ┌─────┼─────┐
 *   ▼     ▼     ▼
 *  Worker Worker Worker ...           (platform or virtual threads)
 *         │
 *         ▼
 *   OffsetTracker → PeriodicCommitter (sliding-window offset tracking)
 * </pre>
 *
 * <h2>Features</h2>
 *
 * <ul>
 *   <li><b>Concurrent processing</b> — configurable worker pool using {@link
 *       io.github.openlogiclab.kafkapipeline.ThreadMode#PLATFORM PLATFORM} or {@link
 *       io.github.openlogiclab.kafkapipeline.ThreadMode#VIRTUAL VIRTUAL} threads
 *   <li><b>Backpressure</b> — record-count hysteresis (high/low watermark) pauses the consumer when
 *       workers fall behind
 *   <li><b>Error handling</b> — retry with exponential backoff, optional DLQ, then {@link
 *       io.github.openlogiclab.kafkapipeline.error.FinalFailureHandler} and skip
 *   <li><b>Offset management</b> — unordered sliding-window tracker with monotonic commit
 *       guarantees and periodic async commits
 *   <li><b>Lifecycle hooks</b> — {@link
 *       io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook} for deduplication,
 *       metrics, tracing, or audit logging
 *   <li><b>Graceful shutdown</b> — on {@link #stop()} or SIGTERM, records already being processed
 *       get up to {@code shutdownTimeout} to finish and completed offsets are committed; records
 *       still queued are not started and are redelivered on the next start. On rebalance, in-flight
 *       records of revoked partitions get up to {@code drainTimeout}.
 * </ul>
 *
 * <h2>Quick Start</h2>
 *
 * <pre>{@code
 * KafkaPipeline<String, String> pipeline = new KafkaPipeline<>(
 *     KafkaPipeline.<String, String>builder()
 *         .consumerProperties(kafkaProps)
 *         .topic("orders")
 *         .handler(record -> myService.process(record))
 *         .concurrency(ThreadMode.VIRTUAL, 200)
 *         .errorStrategy(ErrorStrategy.<String, String>builder()
 *             .maxRetries(3)
 *             .retryBackoff(Duration.ofSeconds(1))
 *             .exponentialBackoff(true)
 *             .dlqHandler((record, error) -> dlqProducer.send(record))
 *             .build())
 *         .build());
 *
 * // Blocking — runs until stop() or SIGTERM:
 * pipeline.start();
 *
 * // Or non-blocking:
 * Thread t = pipeline.startAsync();
 * // ... later ...
 * pipeline.stop();
 * pipeline.awaitShutdown();
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>{@link #start()} and {@link #startAsync()} may only be called once. {@link #stop()} and {@link
 * #isRunning()} are safe to call from any thread.
 *
 * @param <K> record key type
 * @param <V> record value type
 * @see PipelineConfig
 * @see io.github.openlogiclab.kafkapipeline.handler.RecordHandler
 * @see io.github.openlogiclab.kafkapipeline.error.ErrorStrategy
 * @see io.github.openlogiclab.kafkapipeline.ThreadMode
 */
public final class KafkaPipeline<K, V> {

  private static final System.Logger logger = System.getLogger(KafkaPipeline.class.getName());

  private final PipelineConfig<K, V> config;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private volatile Throwable failure;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  private final boolean ownsConsumer;

  private final OffsetTracker offsetTracker;
  private final InFlightCounter counter;
  private final RetryExecutor<K, V> retryExecutor;
  private final RecordDispatcher<K, V> dispatcher;
  private final BackpressureController backpressure;
  private final WorkerPool<K, V> workerPool;
  private final PipelineMetricsCollector metricsCollector;

  private volatile Consumer<K, V> consumer;
  private PeriodicCommitter committer;
  private PartitionPauseController pauseController;
  private PipelineRebalanceListener rebalanceListener;
  private Thread shutdownHook;

  /**
   * Creates a new pipeline with the given configuration.
   *
   * @param config the pipeline configuration
   */
  public KafkaPipeline(PipelineConfig<K, V> config) {
    this(config, null, true);
  }

  /**
   * Package-private constructor for testing — accepts a pre-built {@link Consumer} (e.g. {@code
   * MockConsumer}) instead of creating a {@link KafkaConsumer} internally. The caller retains
   * ownership of the consumer's lifecycle (close is not called).
   */
  KafkaPipeline(PipelineConfig<K, V> config, Consumer<K, V> consumer) {
    this(config, consumer, false);
  }

  /**
   * Creates the pipeline. With a {@code null} consumer, a {@link KafkaConsumer} is created on
   * start. Package-private so tests can control {@code ownsConsumer} to exercise the consumer close
   * path.
   */
  KafkaPipeline(PipelineConfig<K, V> config, Consumer<K, V> consumer, boolean ownsConsumer) {
    this.config = config;
    this.consumer = consumer;
    this.ownsConsumer = ownsConsumer;

    this.offsetTracker =
        new UnorderedOffsetTracker(config.maxWindowSize(), config.skipCompletedOnRestart());
    this.counter = new InFlightCounter();
    this.backpressure = buildBackpressureController(config, counter);
    this.metricsCollector = new PipelineMetricsCollector(counter, backpressure, offsetTracker);
    this.retryExecutor =
        new RetryExecutor<>(config.errorStrategy(), metricsCollector, config.processingTimeout());
    this.dispatcher =
        new RecordDispatcher<>(
            config.dispatchQueueCapacity(),
            config.maxConcurrencyPerPartition(),
            config.concurrency(),
            config.ordering());
    this.workerPool =
        buildWorkerPool(
            config, retryExecutor, offsetTracker, dispatcher, counter, metricsCollector);
  }

  /**
   * Returns a new configuration builder.
   *
   * @param <K> record key type
   * @param <V> record value type
   * @return new builder instance
   */
  public static <K, V> PipelineConfig.Builder<K, V> builder() {
    return PipelineConfig.builder();
  }

  /**
   * Starts the pipeline. Blocks the calling thread until {@link #stop()} is called or the pipeline
   * fails.
   *
   * <p>Before starting any worker, the pipeline checks that the consumer can reach the brokers (a
   * metadata request bounded by the consumer's {@code default.api.timeout.ms}). If it cannot, or if
   * the pipeline later hits an error it cannot recover from (bad credentials or ACLs, a fenced or
   * invalid group member, an unsupported broker, a partition with no offset and {@code
   * auto.offset.reset=none}, or a JVM error such as {@link OutOfMemoryError}), it shuts down,
   * {@link #isRunning()} turns false, and this method rethrows the cause. The cause is also
   * available from {@link #failure()}.
   *
   * @throws IllegalStateException if the pipeline was already started
   * @throws RuntimeException the error that stopped the pipeline
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Pipeline already started");
    }
    running.set(true);

    try {
      initConsumer();
      verifyBrokerConnection();
      workerPool.onFatalError(this::fail);
      workerPool.start();
      committer.start();
      registerShutdownHook();

      String mode = config.isBatchMode() ? "batch" : "per-record";
      logger.log(
          System.Logger.Level.INFO,
          "Pipeline started: topics={0}, concurrency={1}, mode={2}",
          config.topics(),
          config.concurrency(),
          mode);

      new PollLoop<>(
              consumer,
              config.pollTimeout(),
              committer,
              backpressure,
              pauseController,
              workerPool,
              metricsCollector,
              rebalanceListener::retryPendingInit)
          .run(running::get);
    } catch (WakeupException e) {
      // stop() woke a blocking startup call (such as the broker check): a normal stop.
      if (running.get()) fail(e);
    } catch (Throwable t) {
      fail(t);
    } finally {
      running.set(false);
      shutdown();
      shutdownLatch.countDown();
    }

    Throwable cause = failure;
    if (cause instanceof RuntimeException re) throw re;
    if (cause instanceof Error err) throw err;
    if (cause != null) throw new IllegalStateException("Pipeline failed", cause);
  }

  /**
   * Starts the pipeline on a daemon thread. Returns immediately. If the pipeline fails, the thread
   * ends and the cause is available from {@link #failure()}.
   *
   * @return the daemon thread running the pipeline
   */
  public Thread startAsync() {
    Thread thread =
        new Thread(
            () -> {
              try {
                start();
              } catch (Throwable t) {
                // Already logged and kept in failure().
              }
            });
    thread.setName("kafka-pipeline-main");
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /**
   * Returns the error that stopped the pipeline, if it failed.
   *
   * @return the cause, or empty if the pipeline has not failed
   */
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  /** Records the first fatal error and stops the pipeline. Safe to call from any thread. */
  private void fail(Throwable t) {
    synchronized (this) {
      if (failure == null) failure = t;
    }
    logger.log(System.Logger.Level.ERROR, "Pipeline failed, shutting down", t);
    stop();
  }

  /**
   * Fails fast when the brokers are unreachable or reject the client: there is no point starting
   * workers that would wait forever for records.
   */
  private void verifyBrokerConnection() {
    Map<String, ?> topics = consumer.listTopics();
    for (String topic : config.topics()) {
      if (topics != null && !topics.containsKey(topic)) {
        logger.log(
            System.Logger.Level.WARNING,
            "Topic {0} does not exist yet; the pipeline waits until it is created",
            topic);
      }
    }
  }

  /**
   * Signals the pipeline to stop gracefully. The poll loop exits, records already being processed
   * get up to {@code shutdownTimeout} to finish, and completed offsets are committed. Queued
   * records that have not started are not processed; they are redelivered on the next start.
   */
  public void stop() {
    if (running.compareAndSet(true, false)) {
      logger.log(System.Logger.Level.INFO, "Pipeline stop requested");
      if (consumer != null) {
        consumer.wakeup();
      }
    }
  }

  /**
   * Blocks until the pipeline has fully shut down.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  /**
   * Returns whether the pipeline is running.
   *
   * @return true if the pipeline is running
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Returns a point-in-time snapshot of pipeline operational metrics.
   *
   * <p>This method is safe to call from any thread at any time, including before {@link #start()}.
   * Each call reads the current counter values — no background threads or timers involved.
   *
   * @return current metrics snapshot
   * @see PipelineMetrics
   */
  public PipelineMetrics metrics() {
    return metricsCollector.snapshot();
  }

  private static <K, V> BackpressureController buildBackpressureController(
      PipelineConfig<K, V> config, InFlightCounter counter) {
    List<BackpressureSensor> sensors = new ArrayList<>();
    sensors.add(new RecordCountSensor(config.backpressure(), counter));
    if (config.byteBackpressure().enabled()) {
      sensors.add(new ByteSizeSensor(config.byteBackpressure(), counter));
    }
    if (config.heapBackpressure().enabled()) {
      sensors.add(new HeapSensor(config.heapBackpressure()));
    }
    sensors.addAll(config.customSensors());
    return new BackpressureController(config.backpressure(), sensors);
  }

  private static <K, V> WorkerPool<K, V> buildWorkerPool(
      PipelineConfig<K, V> config,
      RetryExecutor<K, V> retryExecutor,
      OffsetTracker offsetTracker,
      RecordDispatcher<K, V> dispatcher,
      InFlightCounter counter,
      PipelineMetricsCollector metricsCollector) {
    if (config.isBatchMode()) {
      BatchWorkerPool<K, V> pool =
          new BatchWorkerPool<>(
              config.concurrency(),
              config.threadMode(),
              config.batchHandler(),
              retryExecutor,
              offsetTracker,
              counter,
              config.dispatchQueueCapacity(),
              metricsCollector,
              config.ordering());
      // Same per-partition limit (with its fair-share floor) as per-record mode.
      pool.partitionLimit(dispatcher::effectivePartitionLimit);
      return pool;
    }
    return new SingleRecordWorkerPool<>(
        config.concurrency(),
        config.threadMode(),
        config.handler(),
        config.lifecycleHook(),
        retryExecutor,
        offsetTracker,
        dispatcher,
        counter,
        metricsCollector);
  }

  private void initConsumer() {
    if (consumer == null) {
      Properties props = new Properties();
      props.putAll(config.consumerProperties());
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      consumer = new KafkaConsumer<>(props);
    }

    pauseController = new PartitionPauseController(consumer, offsetTracker, workerPool::canResume);

    committer =
        new PeriodicCommitter(offsetTracker, consumer, config.commitInterval(), metricsCollector);

    rebalanceListener =
        new PipelineRebalanceListener(
            offsetTracker,
            dispatcher,
            committer::commitSync,
            counter,
            consumer,
            config.drainTimeout(),
            metricsCollector,
            config.skipCompletedOnRestart());

    pauseController.addHold(rebalanceListener::isPendingInit);
    pauseController.addHold(workerPool::holdsPartition);
    consumer.subscribe(config.topics(), rebalanceListener);
  }

  private void shutdown() {
    logger.log(System.Logger.Level.INFO, "Pipeline shutting down...");

    try {
      workerPool.stop(config.shutdownTimeout().toMillis());
    } catch (Exception e) {
      logger.log(System.Logger.Level.WARNING, "Error stopping workers", e);
    }
    retryExecutor.close();

    if (committer != null) {
      try {
        committer.commitSync();
        committer.stop();
      } catch (Exception e) {
        logger.log(System.Logger.Level.WARNING, "Error during final commit", e);
      }
    }

    if (rebalanceListener != null) {
      try {
        rebalanceListener.shutdown();
      } catch (Exception e) {
        logger.log(System.Logger.Level.WARNING, "Error shutting down rebalance listener", e);
      }
    }

    if (ownsConsumer && consumer != null) {
      try {
        consumer.close(
            org.apache.kafka.clients.consumer.CloseOptions.timeout(config.shutdownTimeout()));
      } catch (Exception e) {
        logger.log(System.Logger.Level.WARNING, "Error closing consumer", e);
      }
    }

    removeShutdownHook();

    logger.log(System.Logger.Level.INFO, "Pipeline shut down complete");
  }

  private void registerShutdownHook() {
    shutdownHook =
        new Thread(
            () -> {
              logger.log(System.Logger.Level.INFO, "Shutdown hook triggered");
              stop();
              try {
                awaitShutdown();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "kafka-pipeline-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  private void removeShutdownHook() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        // JVM is already shutting down, ignore
      }
    }
  }
}
