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
package io.github.openlogiclab.kafkapipeline.offset;

import java.time.Duration;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;

/**
 * Offset tracker for unordered processing.
 *
 * <p>Records within a partition can be processed concurrently and acked in any order. Uses a
 * per-partition {@link PartitionWindow} (sliding window) to track individual offset states. The
 * committable offset only advances past contiguous completed entries, preventing data loss on
 * crash/rebalance.
 */
public final class UnorderedOffsetTracker implements OffsetTracker {

  private static final System.Logger logger =
      System.getLogger(UnorderedOffsetTracker.class.getName());

  private final ConcurrentHashMap<TopicPartition, PartitionWindow> partitions =
      new ConcurrentHashMap<>();

  private final int maxWindowSize;
  private volatile boolean trackCompleted;
  private final AtomicLong nextGeneration = new AtomicLong();

  /** Creates a new unordered offset tracker. */
  public UnorderedOffsetTracker() {
    this(PartitionWindow.DEFAULT_MAX_WINDOW_SIZE);
  }

  /**
   * Creates a tracker whose partition windows hold at most {@code maxWindowSize} entries.
   *
   * @param maxWindowSize per-partition window capacity
   */
  public UnorderedOffsetTracker(int maxWindowSize) {
    this(maxWindowSize, false);
  }

  /**
   * Creates a tracker.
   *
   * @param maxWindowSize per-partition window capacity
   * @param trackCompleted whether commit points record offsets that finished beyond the committed
   *     one, and whether {@link #initPartition(TopicPartition, long, String)} skips the ones a
   *     previous owner recorded
   */
  public UnorderedOffsetTracker(int maxWindowSize, boolean trackCompleted) {
    if (maxWindowSize <= 0) {
      throw new IllegalArgumentException("maxWindowSize must be positive, got " + maxWindowSize);
    }
    this.maxWindowSize = maxWindowSize;
    this.trackCompleted = trackCompleted;
  }

  @Override
  public void register(TopicPartition tp, long offset) {
    getOrThrow(tp).register(offset);
  }

  @Override
  public void registerBatch(TopicPartition tp, long[] offsets) {
    if (Objects.isNull(offsets)) {
      throw new IllegalArgumentException("offset cannot be Null");
    }
    if (offsets.length == 0) {
      throw new IllegalArgumentException("offset cannot be empty");
    }
    getOrThrow(tp).registerBatch(offsets);
  }

  @Override
  public boolean registerUnlessDone(TopicPartition tp, long offset) {
    return getOrThrow(tp).registerUnlessDone(offset);
  }

  @Override
  public long[] registerBatchUnlessDone(TopicPartition tp, long[] offsets) {
    if (Objects.isNull(offsets) || offsets.length == 0) {
      throw new IllegalArgumentException("offsets cannot be null or empty");
    }
    return getOrThrow(tp).registerBatchUnlessDone(offsets);
  }

  @Override
  public void markInProgress(TopicPartition tp, long offset) {
    getOrThrow(tp).markInProgress(offset);
  }

  @Override
  public int markBatchInProgress(TopicPartition tp, long[] offsets) {
    return getOrThrow(tp).markBatchInProgress(offsets);
  }

  @Override
  public void ack(TopicPartition tp, long offset) {
    getOrThrow(tp).ack(offset);
  }

  @Override
  public int ackBatch(TopicPartition tp, long[] offsets) {
    return getOrThrow(tp).ackBatch(offsets);
  }

  @Override
  public long generation(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.generation() : NO_GENERATION;
  }

  @Override
  public void markInProgress(TopicPartition tp, long offset, long generation) {
    getOrThrow(tp, generation).markInProgress(offset);
  }

  @Override
  public int markBatchInProgress(TopicPartition tp, long[] offsets, long generation) {
    return getOrThrow(tp, generation).markBatchInProgress(offsets);
  }

  @Override
  public void ack(TopicPartition tp, long offset, long generation) {
    getOrThrow(tp, generation).ack(offset);
  }

  @Override
  public int ackBatch(TopicPartition tp, long[] offsets, long generation) {
    return getOrThrow(tp, generation).ackBatch(offsets);
  }

  @Override
  public OptionalLong getCommittableOffset(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    if (Objects.isNull(window)) {
      return OptionalLong.empty();
    }
    return window.getCommittableOffset();
  }

  @Override
  public Map<TopicPartition, CommitPoint> getUncommittedCommitPoints() {
    Map<TopicPartition, CommitPoint> result = new HashMap<>();
    partitions.forEach(
        (tp, window) -> window.getUncommittedCommitPoint().ifPresent(p -> result.put(tp, p)));
    return result;
  }

  @Override
  public Map<TopicPartition, CommitPoint> getAllCommitPoints() {
    Map<TopicPartition, CommitPoint> result = new HashMap<>();
    partitions.forEach((tp, window) -> window.getCommitPoint().ifPresent(p -> result.put(tp, p)));
    return result;
  }

  @Override
  public void stopTrackingCompleted() {
    trackCompleted = false;
    partitions.values().forEach(PartitionWindow::stopTrackingCompleted);
  }

  @Override
  public void markCommitted(TopicPartition tp, CommitPoint point) {
    PartitionWindow window = partitions.get(tp);
    if (window != null) {
      window.markCommitted(point);
    }
  }

  @Override
  public void initPartition(TopicPartition tp, long startOffset) {
    initPartition(tp, startOffset, null);
  }

  @Override
  public void initPartition(TopicPartition tp, long startOffset, String committedMetadata) {
    BitSet preDone =
        trackCompleted
            ? CompletedOffsetsCodec.decode(committedMetadata, startOffset)
            : new BitSet();
    PartitionWindow window =
        new PartitionWindow(
            startOffset, maxWindowSize, nextGeneration.incrementAndGet(), trackCompleted, preDone);
    PartitionWindow existing = partitions.putIfAbsent(tp, window);
    if (existing != null) {
      throw new IllegalStateException(
          "Partition " + tp + " already initialized. Call clearPartition first.");
    }
    if (!preDone.isEmpty()) {
      logger.log(
          System.Logger.Level.INFO,
          "{0}: {1} records after offset {2} were already processed by the previous owner and will"
              + " be skipped",
          tp,
          preDone.cardinality(),
          startOffset);
    }
  }

  @Override
  public PartitionDrainResult drainPartition(TopicPartition tp, Duration timeout) {
    PartitionWindow window = partitions.get(tp);
    if (window == null) {
      return new PartitionDrainResult(true, 0, 0, OptionalLong.empty());
    }
    return window.drain(timeout);
  }

  @Override
  public void clearPartition(TopicPartition tp) {
    PartitionWindow window = partitions.remove(tp);
    if (window != null) {
      int pending = window.pendingCount();
      int inProgress = window.inProgressCount();
      if (pending > 0 || inProgress > 0) {
        logger.log(
            System.Logger.Level.WARNING,
            "Clearing partition {0} with {1} pending and {2} in-progress. "
                + "These records will be re-consumed by the new owner.",
            tp,
            pending,
            inProgress);
      }
    }
  }

  @Override
  public int pendingCount(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.pendingCount() : 0;
  }

  @Override
  public int inProgressCount(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.inProgressCount() : 0;
  }

  @Override
  public long lag(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.lag() : 0;
  }

  @Override
  public boolean isFailed(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null && window.isFailed();
  }

  @Override
  public void markFailed(TopicPartition tp) {
    getOrThrow(tp).markFailed();
  }

  @Override
  public int failureCount(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.failureCount() : 0;
  }

  @Override
  public int windowSize(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.windowSize() : 0;
  }

  @Override
  public long commitStallMillis(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    return window != null ? window.commitStallMillis() : 0;
  }

  @Override
  public int maxWindowSize() {
    return maxWindowSize;
  }

  private PartitionWindow getOrThrow(TopicPartition tp, long generation) {
    PartitionWindow window = getOrThrow(tp);
    if (generation != NO_GENERATION && window.generation() != generation) {
      throw new IllegalStateException(
          "Partition "
              + tp
              + " was re-assigned since this record was fetched (generation "
              + generation
              + ", now "
              + window.generation()
              + ")");
    }
    return window;
  }

  private PartitionWindow getOrThrow(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    if (window == null) {
      throw new IllegalStateException(
          "Partition " + tp + " not initialized. Call initPartition first.");
    }
    return window;
  }
}
