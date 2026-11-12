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

import io.github.openlogiclab.kafkapipeline.internal.CommitMetrics;
import io.github.openlogiclab.kafkapipeline.offset.CommitPoint;
import io.github.openlogiclab.kafkapipeline.offset.CommitSource;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
 * another thread). The poll loop calls {@link #maybeCommitAsync()} on every iteration; it commits
 * when the commit interval has elapsed, so commits always run on the poll thread and no timer
 * thread is needed.
 *
 * <p>Uses {@code commitAsync()} for periodic commits (non-blocking on the poll loop) and {@code
 * commitSync()} for the final commit during shutdown/rebalance.
 */
public final class PeriodicCommitter {

  private static final System.Logger logger = System.getLogger(PeriodicCommitter.class.getName());

  private final CommitSource offsetTracker;
  private final Consumer<?, ?> consumer;
  private final Duration commitInterval;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private long nextCommitNanos; // poll thread only
  private final CommitMetrics metrics;

  /**
   * Creates a periodic committer.
   *
   * @param offsetTracker the offset tracker to read committable offsets from
   * @param consumer the Kafka consumer to commit offsets with
   * @param commitInterval the interval between periodic commits
   * @param metrics the metrics this component records
   */
  public PeriodicCommitter(
      CommitSource offsetTracker,
      Consumer<?, ?> consumer,
      Duration commitInterval,
      CommitMetrics metrics) {
    this.offsetTracker = offsetTracker;
    this.consumer = consumer;
    this.commitInterval = commitInterval;
    this.metrics = metrics;
  }

  /** Starts periodic commits; the first one is due one interval from now. */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("PeriodicCommitter already started");
    }
    long intervalMs = commitInterval.toMillis();
    // A deadline checked by the poll thread replaces a timer thread that only flipped a flag.
    nextCommitNanos = System.nanoTime() + commitInterval.toNanos();
    logger.log(
        System.Logger.Level.INFO, "PeriodicCommitter started with interval {0}ms", intervalMs);
  }

  /** Stops periodic commits. */
  public void stop() {
    running.set(false);
    logger.log(System.Logger.Level.INFO, "PeriodicCommitter stopped");
  }

  /**
   * Called by the poll loop to check if a commit is due and perform it. Must be called from the
   * poll thread (the same thread that calls {@code consumer.poll()}).
   */
  public void maybeCommitAsync() {
    if (!running.get()) return;
    long now = System.nanoTime();
    if (now - nextCommitNanos < 0) return;
    // Fixed rate, but never try to catch up on ticks missed while the poll thread was busy.
    long interval = commitInterval.toNanos();
    nextCommitNanos += interval;
    if (now - nextCommitNanos >= 0) nextCommitNanos = now + interval;
    commitAsync();
  }

  /** Performs an async commit. Must be called from the poll thread. */
  public void commitAsync() {
    if (!running.get()) return;
    try {
      Map<TopicPartition, OffsetAndMetadata> offsets =
          toKafka(offsetTracker.getUncommittedCommitPoints());
      if (offsets.isEmpty()) {
        logger.log(System.Logger.Level.DEBUG, "No uncommitted offsets, skipping commit");
        return;
      }

      consumer.commitAsync(
          offsets,
          (committedOffsets, exception) -> {
            if (exception != null) {
              metrics.recordCommitFailure();
              if (metadataTooLarge(exception)) return;
              logger.log(
                  System.Logger.Level.WARNING, "Async commit failed: {0}", exception.getMessage());
            } else {
              markCommitted(committedOffsets);
              metrics.recordCommitSuccess();
              logger.log(System.Logger.Level.DEBUG, "Committed offsets: {0}", committedOffsets);
            }
          });
    } catch (Exception e) {
      metrics.recordCommitFailure();
      logger.log(System.Logger.Level.WARNING, "Error during async commit: {0}", e.getMessage());
    }
  }

  /** Performs a synchronous commit. Must be called from the poll thread. */
  public void commitSync() {
    try {
      // Drain and shutdown commit every partition's current position, not only changed ones.
      Map<TopicPartition, OffsetAndMetadata> offsets = toKafka(offsetTracker.getAllCommitPoints());
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
      markCommitted(offsets);
      metrics.recordCommitSuccess();
      logger.log(System.Logger.Level.INFO, "Sync committed offsets: {0}", offsets);
    } catch (Exception e) {
      metrics.recordCommitFailure();
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

  private void markCommitted(Map<TopicPartition, OffsetAndMetadata> committed) {
    committed.forEach(
        (tp, meta) ->
            offsetTracker.markCommitted(tp, new CommitPoint(meta.offset(), meta.metadata())));
  }

  private static Map<TopicPartition, OffsetAndMetadata> toKafka(
      Map<TopicPartition, CommitPoint> points) {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>(points.size());
    points.forEach((tp, p) -> result.put(tp, new OffsetAndMetadata(p.offset(), p.metadata())));
    return result;
  }
}
