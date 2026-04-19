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
import io.github.openlogiclab.kafkapipeline.backpressure.RecordCountSensor;
import io.github.openlogiclab.kafkapipeline.dispatch.PeriodicCommitter;
import io.github.openlogiclab.kafkapipeline.dispatch.PipelineRebalanceListener;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import io.github.openlogiclab.kafkapipeline.worker.BatchWorkerPool;
import io.github.openlogiclab.kafkapipeline.worker.RetryExecutor;
import io.github.openlogiclab.kafkapipeline.worker.SingleRecordWorkerPool;
import io.github.openlogiclab.kafkapipeline.worker.WorkerPool;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Main entry point for the Kafka Pipeline library. Wraps a {@link KafkaConsumer} with concurrent
 * out-of-order processing, record-count backpressure, configurable error handling (retry → DLQ →
 * fallback), lifecycle hooks, and automatic offset management — so users only need to focus on
 * processing each record.
 *
 * <h2>Architecture</h2>
 *
 * <pre>
 *   KafkaConsumer.poll()              (single-threaded poll loop)
 *         │
 *         ▼
 *   RecordDispatcher                  (per-partition BlockingQueues)
 *         │
 *   ┌─────┼─────┐
 *   ▼     ▼     ▼
 *  Worker Worker Worker ...           (platform or virtual threads)
 *         │
 *         ▼
 *   OffsetTracker → PeriodicCommitter (unordered sliding-window offset tracking)
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
 *   <li><b>Error handling</b> — retry with exponential backoff, optional DLQ hook, then skip or
 *       fail-partition as a final fallback
 *   <li><b>Offset management</b> — unordered sliding-window tracker with monotonic commit
 *       guarantees and periodic async commits
 *   <li><b>Lifecycle hooks</b> — {@link
 *       io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook} for deduplication,
 *       metrics, tracing, or audit logging
 *   <li><b>Graceful shutdown</b> — drains in-flight records and commits offsets on {@link #stop()},
 *       SIGTERM, or rebalance
 * </ul>
 *
 * <h2>Quick Start</h2>
 *
 * <pre>{@code
 * KafkaPipeline<String, String> pipeline = new KafkaPipeline<>(
 *     KafkaPipeline.<String, String>builder()
 *         .consumerProperties(kafkaProps)
 *         .topics("orders", "payments")
 *         .handler(record -> myService.process(record))
 *         .concurrency(ThreadMode.VIRTUAL, 200)
 *         .errorStrategy(ErrorStrategy.<String, String>builder()
 *             .maxRetries(3)
 *             .retryBackoff(Duration.ofSeconds(1))
 *             .exponentialBackoff(true)
 *             .dlqHandler((record, error) -> dlqProducer.send(record))
 *             .fallback(Fallback.SKIP)
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

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  private final boolean ownsConsumer;

  private final OffsetTracker offsetTracker;
  private final InFlightCounter counter;
  private final RetryExecutor<K, V> retryExecutor;
  private final RecordDispatcher<K, V> dispatcher;
  private final BackpressureController backpressure;
  private final WorkerPool<K, V> workerPool;

  private Consumer<K, V> consumer;
  private PeriodicCommitter committer;

  public KafkaPipeline(PipelineConfig<K, V> config) {
    this.config = config;
    this.consumer = null;
    this.ownsConsumer = true;

    this.offsetTracker = new UnorderedOffsetTracker();
    this.counter = new InFlightCounter();
    this.retryExecutor = new RetryExecutor<>(config.errorStrategy());
    this.dispatcher = new RecordDispatcher<>(config.dispatchQueueCapacity());
    this.backpressure = buildBackpressureController(config, counter);
    this.workerPool = buildWorkerPool(config, retryExecutor, offsetTracker, dispatcher, counter);
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
   * Package-private constructor for testing — allows controlling {@code ownsConsumer} to exercise
   * the consumer close path during shutdown tests.
   */
  KafkaPipeline(PipelineConfig<K, V> config, Consumer<K, V> consumer, boolean ownsConsumer) {
    this.config = config;
    this.consumer = consumer;
    this.ownsConsumer = ownsConsumer;

    this.offsetTracker = new UnorderedOffsetTracker();
    this.counter = new InFlightCounter();
    this.retryExecutor = new RetryExecutor<>(config.errorStrategy());
    this.dispatcher = new RecordDispatcher<>(config.dispatchQueueCapacity());
    this.backpressure = buildBackpressureController(config, counter);
    this.workerPool = buildWorkerPool(config, retryExecutor, offsetTracker, dispatcher, counter);
  }

  public static <K, V> PipelineConfig.Builder<K, V> builder() {
    return PipelineConfig.builder();
  }

  /** Start the pipeline. Blocks the calling thread until {@link #stop()} is called. */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Pipeline already started");
    }

    try {
      initConsumer();
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

      pollLoop();
    } catch (Exception e) {
      logger.log(System.Logger.Level.ERROR, "Pipeline error", e);
    } finally {
      shutdown();
      shutdownLatch.countDown();
    }
  }

  /** Start the pipeline on a daemon thread. Returns immediately. */
  public Thread startAsync() {
    Thread thread = new Thread(this::start);
    thread.setName("kafka-pipeline-main");
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /**
   * Signal the pipeline to stop gracefully. The poll loop will exit, workers will drain, and
   * offsets will be committed.
   */
  public void stop() {
    if (running.compareAndSet(true, false)) {
      logger.log(System.Logger.Level.INFO, "Pipeline stop requested");
      if (consumer != null) {
        consumer.wakeup();
      }
    }
  }

  /** Block until the pipeline has fully shut down. */
  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  public boolean isRunning() {
    return running.get();
  }

  private static <K, V> BackpressureController buildBackpressureController(
      PipelineConfig<K, V> config, InFlightCounter counter) {
    List<BackpressureSensor> sensors = new ArrayList<>();
    sensors.add(new RecordCountSensor(config.backpressure(), counter));
    if (config.byteBackpressure().enabled()) {
      sensors.add(new ByteSizeSensor(config.byteBackpressure(), counter));
    }
    sensors.addAll(config.customSensors());
    return new BackpressureController(config.backpressure(), sensors);
  }

  private static <K, V> WorkerPool<K, V> buildWorkerPool(
      PipelineConfig<K, V> config,
      RetryExecutor<K, V> retryExecutor,
      OffsetTracker offsetTracker,
      RecordDispatcher<K, V> dispatcher,
      InFlightCounter counter) {
    if (config.isBatchMode()) {
      return new BatchWorkerPool<>(
          config.concurrency(),
          config.threadMode(),
          config.batchHandler(),
          retryExecutor,
          offsetTracker,
          counter,
          config.dispatchQueueCapacity());
    }
    return new SingleRecordWorkerPool<>(
        config.concurrency(),
        config.threadMode(),
        config.handler(),
        config.lifecycleHook(),
        retryExecutor,
        offsetTracker,
        dispatcher,
        counter);
  }

  private void initConsumer() {
    if (consumer == null) {
      Properties props = new Properties();
      props.putAll(config.consumerProperties());
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      consumer = new KafkaConsumer<>(props);
    }

    committer = new PeriodicCommitter(offsetTracker, consumer, config.commitInterval());

    PipelineRebalanceListener rebalanceListener =
        new PipelineRebalanceListener(
            offsetTracker,
            dispatcher,
            committer::commitSync,
            counter,
            consumer,
            config.drainTimeout());

    consumer.subscribe(config.topics(), rebalanceListener);
  }

  private void pollLoop() {
    boolean paused = false;

    while (running.get()) {
      try {
        if (backpressure.shouldThrottle()) {
          if (!paused) {
            consumer.pause(consumer.assignment());
            paused = true;
            logger.log(
                System.Logger.Level.DEBUG,
                "Backpressure: paused. {0}",
                backpressure.statusSummary());
          }
          consumer.poll(config.pollTimeout());
          continue;
        }

        if (paused) {
          consumer.resume(consumer.assignment());
          paused = false;
          logger.log(System.Logger.Level.DEBUG, "Backpressure: resumed");
        }

        ConsumerRecords<K, V> records = consumer.poll(config.pollTimeout());
        if (records.isEmpty()) continue;

        workerPool.dispatch(records);
      } catch (org.apache.kafka.common.errors.WakeupException e) {
        if (running.get()) {
          logger.log(System.Logger.Level.WARNING, "Unexpected wakeup in poll loop");
        }
      } catch (Exception e) {
        logger.log(System.Logger.Level.ERROR, "Error in poll loop", e);
        if (running.get()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
  }

  private void shutdown() {
    logger.log(System.Logger.Level.INFO, "Pipeline shutting down...");

    try {
      workerPool.stop(config.shutdownTimeout().toMillis());
    } catch (Exception e) {
      logger.log(System.Logger.Level.WARNING, "Error stopping workers", e);
    }

    try {
      committer.commitSync();
      committer.stop();
    } catch (Exception e) {
      logger.log(System.Logger.Level.WARNING, "Error during final commit", e);
    }

    if (ownsConsumer) {
      try {
        consumer.close(
            org.apache.kafka.clients.consumer.CloseOptions.timeout(config.shutdownTimeout()));
      } catch (Exception e) {
        logger.log(System.Logger.Level.WARNING, "Error closing consumer", e);
      }
    }

    logger.log(System.Logger.Level.INFO, "Pipeline shut down complete");
  }

  private void registerShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
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
                "kafka-pipeline-shutdown"));
  }
}
