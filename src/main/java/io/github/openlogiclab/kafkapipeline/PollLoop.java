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
import io.github.openlogiclab.kafkapipeline.dispatch.PartitionPauseController;
import io.github.openlogiclab.kafkapipeline.dispatch.PeriodicCommitter;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.worker.WorkerPool;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

/**
 * The poll thread's loop: commit when due, pause or resume for backpressure, poll, and hand the
 * records to the worker pool. Runs until the given condition turns false.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
final class PollLoop<K, V> {

  private static final System.Logger logger = System.getLogger(PollLoop.class.getName());
  private static final long ERROR_BACKOFF_MS = 100;

  private final Consumer<K, V> consumer;
  private final Duration pollTimeout;
  private final PeriodicCommitter committer;
  private final BackpressureController backpressure;
  private final PartitionPauseController pauseController;
  private final WorkerPool<K, V> workerPool;
  private final PipelineMetricsCollector metricsCollector;
  private final Runnable beforePoll;
  private boolean throttled;

  PollLoop(
      Consumer<K, V> consumer,
      Duration pollTimeout,
      PeriodicCommitter committer,
      BackpressureController backpressure,
      PartitionPauseController pauseController,
      WorkerPool<K, V> workerPool,
      PipelineMetricsCollector metricsCollector) {
    this(
        consumer,
        pollTimeout,
        committer,
        backpressure,
        pauseController,
        workerPool,
        metricsCollector,
        () -> {});
  }

  /**
   * Creates the loop with a hook that runs on the poll thread before each poll, such as retrying
   * partitions whose initialization failed.
   */
  PollLoop(
      Consumer<K, V> consumer,
      Duration pollTimeout,
      PeriodicCommitter committer,
      BackpressureController backpressure,
      PartitionPauseController pauseController,
      WorkerPool<K, V> workerPool,
      PipelineMetricsCollector metricsCollector,
      Runnable beforePoll) {
    this.beforePoll = beforePoll;
    this.consumer = consumer;
    this.pollTimeout = pollTimeout;
    this.committer = committer;
    this.backpressure = backpressure;
    this.pauseController = pauseController;
    this.workerPool = workerPool;
    this.metricsCollector = metricsCollector;
  }

  void run(BooleanSupplier running) {
    while (running.getAsBoolean()) {
      try {
        pollOnce();
      } catch (WakeupException e) {
        if (running.getAsBoolean()) {
          logger.log(System.Logger.Level.WARNING, "Unexpected wakeup in poll loop");
        }
      } catch (Exception e) {
        // Errors that retrying cannot fix end the loop; the pipeline stops and reports them.
        if (FatalErrors.isFatal(e)) throw e;
        logger.log(System.Logger.Level.ERROR, "Error in poll loop", e);
        if (running.getAsBoolean() && !backOff()) break;
      }
    }
  }

  private void pollOnce() {
    // Periodic commits run here: the consumer is not thread-safe.
    committer.maybeCommitAsync();
    beforePoll.run();

    updateThrottle();
    // Reapplied before every poll so partitions assigned by a rebalance are paused too.
    pauseController.apply(throttled);

    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
    metricsCollector.recordPoll();
    if (records.isEmpty()) {
      metricsCollector.recordEmptyPoll();
      return;
    }
    // Never drop polled records: the consumer position has already moved past them. Records that
    // do not fit in their partition's window are fetched again after a seek.
    pauseController.seekBack(workerPool.dispatch(records));
  }

  private void updateThrottle() {
    boolean shouldThrottle = backpressure.shouldThrottle();
    if (shouldThrottle == throttled) return;
    throttled = shouldThrottle;
    if (throttled) metricsCollector.recordThrottle();
    logger.log(
        System.Logger.Level.DEBUG,
        "Backpressure: {0}. {1}",
        throttled ? "paused" : "resumed",
        backpressure.statusSummary());
  }

  /** Waits briefly after an error; returns false if interrupted. */
  private static boolean backOff() {
    try {
      Thread.sleep(ERROR_BACKOFF_MS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
