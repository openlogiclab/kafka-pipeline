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
import io.github.openlogiclab.kafkapipeline.offset.CommitPoint;
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
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.WakeupException;

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

  /**
   * Creates a periodic committer.
   *
   * @param offsetTracker the offset tracker to read committable offsets from
   * @param consumer the Kafka consumer to commit offsets with
   * @param commitInterval the interval between periodic commits
   * @param metricsCollector the metrics collector
   */
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

  /** Starts the periodic commit scheduler. */
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

  /** Stops the periodic commit scheduler and waits for termination. */
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
      // Only get offsets that have uncommitted progress
      Map<TopicPartition, OffsetAndMetadata> offsets = buildUncommittedMap();
      if (offsets.isEmpty()) {
        logger.log(System.Logger.Level.DEBUG, "No uncommitted offsets, skipping commit");
        return;
      }

      consumer.commitAsync(
          offsets,
          (committedOffsets, exception) -> {
            if (exception != null) {
              metricsCollector.recordCommitFailure();
              if (metadataTooLarge(exception)) return;
              logger.log(
                  System.Logger.Level.WARNING, "Async commit failed: {0}", exception.getMessage());
            } else {
              // Mark as committed in offset tracker
              committedOffsets.forEach(
                  (tp, meta) ->
                      offsetTracker.markCommitted(
                          tp, new CommitPoint(meta.offset(), metadataOf(meta))));
              metricsCollector.recordCommitSuccess();
              logger.log(System.Logger.Level.DEBUG, "Committed offsets: {0}", committedOffsets);
            }
          });
    } catch (Exception e) {
      metricsCollector.recordCommitFailure();
      logger.log(System.Logger.Level.WARNING, "Error during async commit: {0}", e.getMessage());
    }
  }

  /** Performs a synchronous commit. Must be called from the poll thread. */
  public void commitSync() {
    try {
      // For sync commits (drain/shutdown), always commit current position
      Map<TopicPartition, OffsetAndMetadata> offsets = buildAllCommittableMap();
      if (offsets.isEmpty()) return;

      try {
        consumer.commitSync(offsets);
      } catch (WakeupException e) {
        // stop() calls wakeup(), and a wakeup that lands while the poll thread is outside poll()
        // fires on the next blocking call, which can be this final commit. The wakeup is
        // consumed by the exception, so retrying once commits normally.
        logger.log(System.Logger.Level.DEBUG, "Sync commit interrupted by wakeup, retrying");
        consumer.commitSync(offsets);
      }
      // Mark as committed
      offsets.forEach(
          (tp, meta) ->
              offsetTracker.markCommitted(tp, new CommitPoint(meta.offset(), metadataOf(meta))));
      metricsCollector.recordCommitSuccess();
      logger.log(System.Logger.Level.INFO, "Sync committed offsets: {0}", offsets);
    } catch (Exception e) {
      metricsCollector.recordCommitFailure();
      if (metadataTooLarge(e) && !retriedWithoutMetadata) {
        // Commit the offsets again, now without metadata, rather than losing this final commit.
        retriedWithoutMetadata = true;
        try {
          commitSync();
        } finally {
          retriedWithoutMetadata = false;
        }
        return;
      }
      logger.log(System.Logger.Level.ERROR, "Sync commit failed: {0}", e.getMessage());
    }
  }

  private boolean retriedWithoutMetadata; // poll thread only

  /**
   * If the broker rejected the commit metadata as too large, stops writing it so later commits go
   * through with plain offsets.
   */
  private boolean metadataTooLarge(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof OffsetMetadataTooLarge) {
        offsetTracker.stopTrackingCompleted();
        logger.log(
            System.Logger.Level.WARNING,
            "The broker rejected commit metadata as too large (offset.metadata.max.bytes). No"
                + " longer recording finished offsets: records that finished after a stuck one"
                + " will be processed again after a restart. Set offset.metadata.max.bytes to at"
                + " least 4096 to keep this, or turn off skipCompletedOnRestart.");
        return true;
      }
    }
    return false;
  }

  private Map<TopicPartition, OffsetAndMetadata> buildUncommittedMap() {
    return toKafka(offsetTracker.getUncommittedCommitPoints());
  }

  private Map<TopicPartition, OffsetAndMetadata> buildAllCommittableMap() {
    return toKafka(offsetTracker.getAllCommitPoints());
  }

  private static Map<TopicPartition, OffsetAndMetadata> toKafka(
      Map<TopicPartition, CommitPoint> points) {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>(points.size());
    points.forEach((tp, p) -> result.put(tp, new OffsetAndMetadata(p.offset(), p.metadata())));
    return result;
  }

  private static String metadataOf(OffsetAndMetadata meta) {
    return meta.metadata() == null ? "" : meta.metadata();
  }
}
