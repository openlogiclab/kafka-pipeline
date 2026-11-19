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
package io.github.openlogiclab.kafkapipeline.commit;

import io.github.openlogiclab.kafkapipeline.internal.Causes;
import io.github.openlogiclab.kafkapipeline.internal.CommitMetrics;
import io.github.openlogiclab.kafkapipeline.offset.CommitPoint;
import io.github.openlogiclab.kafkapipeline.offset.CommitSource;
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
 * Commits offsets through the consumer: asynchronously every {@code commitInterval}, and
 * synchronously after a rebalance drain and at shutdown.
 *
 * <p>Runs on the poll thread only, because the consumer is not thread-safe. The poll loop calls
 * {@link #maybeCommit()} on every iteration, and it commits once the interval has elapsed; there is
 * no timer thread.
 */
public final class PeriodicCommitter implements CommitStrategy {

  private static final System.Logger logger = System.getLogger(PeriodicCommitter.class.getName());

  /**
   * Broker {@code offset.metadata.max.bytes} needed for the completed-offsets metadata: its encoded
   * length is capped at 4,000 characters (the broker's default limit is 4,096).
   */
  static final int MIN_METADATA_BYTES = 4096;

  private final CommitSource offsetTracker;
  private final Consumer<?, ?> consumer;
  private final Duration commitInterval;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private long nextCommitNanos; // poll thread only
  private boolean retriedWithoutMetadata; // poll thread only
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
  @Override
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
  @Override
  public void stop() {
    running.set(false);
    logger.log(System.Logger.Level.INFO, "PeriodicCommitter stopped");
  }

  /** Commits asynchronously if the commit interval has elapsed. Poll thread only. */
  @Override
  public void maybeCommit() {
    if (!running.get()) return;
    long now = System.nanoTime();
    if (now - nextCommitNanos < 0) return;
    // Fixed rate, but never try to catch up on ticks missed while the poll thread was busy.
    long interval = commitInterval.toNanos();
    nextCommitNanos += interval;
    if (now - nextCommitNanos >= 0) nextCommitNanos = now + interval;
    commitAsync();
  }

  /** Commits what changed since the last commit, without waiting. Poll thread only. */
  void commitAsync() {
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

  /** Commits every partition's current position and waits for the result. Poll thread only. */
  @Override
  public void commitNow() {
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
          commitNow();
        } finally {
          retriedWithoutMetadata = false;
        }
        return;
      }
      logger.log(System.Logger.Level.ERROR, "Sync commit failed: {0}", e.getMessage());
    }
  }

  /**
   * If the broker rejected the commit metadata as too large, stops writing it so later commits go
   * through with plain offsets.
   */
  private boolean metadataTooLarge(Throwable e) {
    if (Causes.anyMatch(e, t -> t instanceof OffsetMetadataTooLarge)) {
      offsetTracker.disableSkipCompletedOnRestart();
      logger.log(
          System.Logger.Level.WARNING,
          "The broker rejected commit metadata as too large (offset.metadata.max.bytes). No"
              + " longer recording finished offsets: records that finished after a stuck one"
              + " will be processed again after a restart. Set offset.metadata.max.bytes to at"
              + " least "
              + MIN_METADATA_BYTES
              + " to keep this, or turn off skipCompletedOnRestart.");
      return true;
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
