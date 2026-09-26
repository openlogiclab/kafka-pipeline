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
package io.github.openlogiclab.kafkapipeline.dispatch;

import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Timer-based offset committer that periodically reads committable offsets from the {@link
 * OffsetTracker} and commits them via the Kafka consumer.
 *
 * <p>Thread-safety: Kafka's consumer is not thread-safe (only {@code wakeup()} may be called from
 * another thread). This committer uses a flag-based signaling approach: the scheduler thread sets a
 * {@code commitDue} flag, and the poll loop calls {@link #maybeCommitAsync()} to perform the actual
 * commit on the poll thread.
 *
 * <p>Uses {@code commitAsync()} for periodic commits (non-blocking on the poll loop) and {@code
 * commitSync()} for the final commit during shutdown/rebalance.
 */
public final class PeriodicCommitter {

  private static final System.Logger logger = System.getLogger(PeriodicCommitter.class.getName());

  private final OffsetTracker offsetTracker;
  private final Consumer<?, ?> consumer;
  private final Duration commitInterval;
  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean commitDue = new AtomicBoolean(false);
  private final PipelineMetricsCollector metricsCollector;

  public PeriodicCommitter(
      OffsetTracker offsetTracker,
      Consumer<?, ?> consumer,
      Duration commitInterval,
      PipelineMetricsCollector metricsCollector) {
    this.offsetTracker = offsetTracker;
    this.consumer = consumer;
    this.commitInterval = commitInterval;
    this.metricsCollector = metricsCollector;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r);
              t.setName("kafka-pipeline-committer");
              t.setDaemon(true);
              return t;
            });
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("PeriodicCommitter already started");
    }
    long intervalMs = commitInterval.toMillis();
    scheduler.scheduleAtFixedRate(
        this::signalCommitDue, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    logger.log(
        System.Logger.Level.INFO, "PeriodicCommitter started with interval {0}ms", intervalMs);
  }

  public void stop() {
    running.set(false);
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
    logger.log(System.Logger.Level.INFO, "PeriodicCommitter stopped");
  }

  /** Called by the scheduler thread to signal that a commit is due. Does not touch the consumer. */
  private void signalCommitDue() {
    if (running.get()) {
      commitDue.set(true);
    }
  }

  /**
   * Called by the poll loop to check if a commit is due and perform it. Must be called from the
   * poll thread (the same thread that calls {@code consumer.poll()}).
   */
  public void maybeCommitAsync() {
    if (commitDue.compareAndSet(true, false)) {
      commitAsync();
    }
  }

  /** Performs an async commit. Must be called from the poll thread. */
  public void commitAsync() {
    if (!running.get()) return;
    try {
      Map<TopicPartition, OffsetAndMetadata> offsets = buildCommitMap();
      if (offsets.isEmpty()) return;

      consumer.commitAsync(
          offsets,
          (committedOffsets, exception) -> {
            if (exception != null) {
              metricsCollector.recordCommitFailure();
              logger.log(
                  System.Logger.Level.WARNING, "Async commit failed: {0}", exception.getMessage());
            } else {
              metricsCollector.recordCommitSuccess();
              logger.log(
                  System.Logger.Level.DEBUG,
                  "Committed offsets for {0} partitions",
                  committedOffsets.size());
            }
          });
    } catch (Exception e) {
      metricsCollector.recordCommitFailure();
      logger.log(System.Logger.Level.WARNING, "Error during async commit: {0}", e.getMessage());
    }
  }

  public void commitSync() {
    try {
      Map<TopicPartition, OffsetAndMetadata> offsets = buildCommitMap();
      if (offsets.isEmpty()) return;

      consumer.commitSync(offsets);
      metricsCollector.recordCommitSuccess();
      logger.log(
          System.Logger.Level.INFO, "Sync committed offsets for {0} partitions", offsets.size());
    } catch (Exception e) {
      metricsCollector.recordCommitFailure();
      logger.log(System.Logger.Level.ERROR, "Sync commit failed: {0}", e.getMessage());
    }
  }

  private Map<TopicPartition, OffsetAndMetadata> buildCommitMap() {
    Map<TopicPartition, Long> raw = offsetTracker.getAllCommittableOffsets();
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>(raw.size());
    for (Map.Entry<TopicPartition, Long> entry : raw.entrySet()) {
      result.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
    }
    return result;
  }
}
