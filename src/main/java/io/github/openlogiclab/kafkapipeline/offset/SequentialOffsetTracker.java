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
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.common.TopicPartition;

/**
 * Offset tracker for partition-ordered processing (Mode A).
 *
 * <p>Each partition has at most one record in-progress at any time. Records within a partition are
 * processed strictly in offset order, so the committable offset is simply the last acked offset +
 * 1.
 */
public final class SequentialOffsetTracker implements OffsetTracker {

  private final ConcurrentHashMap<TopicPartition, PartitionState> partitions =
      new ConcurrentHashMap<>();

  private static class PartitionState {
    final ReentrantLock lock = new ReentrantLock();
    final Condition drained = lock.newCondition();

    final long baseOffset;
    long committableOffset;
    long highestRegistered = -1;
    long currentInProgress = -1;
    int pendingCount = 0;
    int completedCount = 0;
    boolean failed = false;

    PartitionState(long startOffset) {
      this.baseOffset = startOffset;
      this.committableOffset = startOffset;
    }
  }

  @Override
  public void register(TopicPartition tp, long offset) {
    PartitionState state = getOrThrow(tp);
    state.lock.lock();
    try {
      validateNotFailed(state, tp);
      if (offset <= state.highestRegistered) {
        throw new IllegalStateException(
            "Offset "
                + offset
                + " <= highest registered "
                + state.highestRegistered
                + " for "
                + tp);
      }
      state.highestRegistered = offset;
      state.pendingCount++;
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void registerBatch(TopicPartition tp, long fromOffset, long toOffset) {
    if (fromOffset > toOffset) {
      throw new IllegalArgumentException("fromOffset " + fromOffset + " > toOffset " + toOffset);
    }
    PartitionState state = getOrThrow(tp);
    state.lock.lock();
    try {
      validateNotFailed(state, tp);
      if (fromOffset <= state.highestRegistered) {
        throw new IllegalStateException(
            "Batch start "
                + fromOffset
                + " <= highest registered "
                + state.highestRegistered
                + " for "
                + tp);
      }
      state.highestRegistered = toOffset;
      state.pendingCount += (int) (toOffset - fromOffset + 1);
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void markInProgress(TopicPartition tp, long offset) {
    PartitionState state = getOrThrow(tp);
    state.lock.lock();
    try {
      validateNotFailed(state, tp);
      if (state.currentInProgress != -1) {
        throw new IllegalStateException(
            "Partition "
                + tp
                + " already has offset "
                + state.currentInProgress
                + " in progress, cannot start "
                + offset);
      }
      if (offset < state.committableOffset || offset > state.highestRegistered) {
        throw new IllegalStateException(
            "Offset "
                + offset
                + " outside registered range ["
                + state.committableOffset
                + ", "
                + state.highestRegistered
                + "] for "
                + tp);
      }
      state.currentInProgress = offset;
      state.pendingCount--;
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void markBatchInProgress(TopicPartition tp, long fromOffset, long toOffset) {
    throw new UnsupportedOperationException(
        "SequentialOffsetTracker does not support batch operations. Use UnorderedOffsetTracker.");
  }

  @Override
  public void ackBatch(TopicPartition tp, long fromOffset, long toOffset) {
    throw new UnsupportedOperationException(
        "SequentialOffsetTracker does not support batch operations. Use UnorderedOffsetTracker.");
  }

  @Override
  public void ack(TopicPartition tp, long offset) {
    PartitionState state = getOrThrow(tp);
    state.lock.lock();
    try {
      if (state.currentInProgress != offset) {
        throw new IllegalStateException(
            "Cannot ack offset "
                + offset
                + " for "
                + tp
                + ", current in-progress is "
                + state.currentInProgress);
      }
      long newCommittable = offset + 1;
      if (newCommittable < state.committableOffset) {
        throw new IllegalStateException(
            "Committable offset would regress from "
                + state.committableOffset
                + " to "
                + newCommittable
                + " for "
                + tp);
      }
      state.committableOffset = newCommittable;
      state.currentInProgress = -1;
      state.completedCount++;
      state.drained.signalAll();
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void fail(TopicPartition tp, long offset) {
    PartitionState state = getOrThrow(tp);
    state.lock.lock();
    try {
      if (state.currentInProgress != offset) {
        throw new IllegalStateException(
            "Cannot fail offset "
                + offset
                + " for "
                + tp
                + ", current in-progress is "
                + state.currentInProgress);
      }
      state.currentInProgress = -1;
      state.failed = true;
      state.drained.signalAll();
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void resolveFailure(TopicPartition tp, long offset) {
    PartitionState state = getOrThrow(tp);
    state.lock.lock();
    try {
      if (!state.failed) {
        throw new IllegalStateException("Partition " + tp + " is not in failed state");
      }
      long newCommittable = offset + 1;
      if (newCommittable < state.committableOffset) {
        throw new IllegalStateException(
            "Committable offset would regress from "
                + state.committableOffset
                + " to "
                + newCommittable
                + " for "
                + tp);
      }
      state.committableOffset = newCommittable;
      state.failed = false;
      state.completedCount++;
      state.drained.signalAll();
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public OptionalLong getCommittableOffset(TopicPartition tp) {
    PartitionState state = partitions.get(tp);
    if (state == null) {
      return OptionalLong.empty();
    }
    state.lock.lock();
    try {
      return state.committableOffset > state.baseOffset
          ? OptionalLong.of(state.committableOffset)
          : OptionalLong.empty();
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public Map<TopicPartition, Long> getAllCommittableOffsets() {
    Map<TopicPartition, Long> result = new HashMap<>();
    for (Map.Entry<TopicPartition, PartitionState> entry : partitions.entrySet()) {
      PartitionState state = entry.getValue();
      state.lock.lock();
      try {
        if (state.committableOffset > state.baseOffset) {
          result.put(entry.getKey(), state.committableOffset);
        }
      } finally {
        state.lock.unlock();
      }
    }
    return result;
  }

  @Override
  public void initPartition(TopicPartition tp, long startOffset) {
    PartitionState existing = partitions.putIfAbsent(tp, new PartitionState(startOffset));
    if (existing != null) {
      throw new IllegalStateException(
          "Partition " + tp + " already initialized. Call clearPartition first.");
    }
  }

  @Override
  public PartitionDrainResult drainPartition(TopicPartition tp, Duration timeout) {
    PartitionState state = partitions.get(tp);
    if (state == null) {
      return new PartitionDrainResult(true, 0, 0, OptionalLong.empty());
    }
    state.lock.lock();
    try {
      long deadline = System.nanoTime() + timeout.toNanos();
      while (state.currentInProgress != -1 && !state.failed) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        state.drained.await(remaining, TimeUnit.NANOSECONDS);
      }
      boolean allCompleted = state.currentInProgress == -1 && state.pendingCount == 0;
      int abandoned = state.pendingCount + (state.currentInProgress != -1 ? 1 : 0);
      OptionalLong committable =
          state.committableOffset > state.baseOffset
              ? OptionalLong.of(state.committableOffset)
              : OptionalLong.empty();
      return new PartitionDrainResult(
          allCompleted && !state.failed, state.completedCount, abandoned, committable);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      int abandoned = state.pendingCount + (state.currentInProgress != -1 ? 1 : 0);
      OptionalLong committable =
          state.committableOffset > state.baseOffset
              ? OptionalLong.of(state.committableOffset)
              : OptionalLong.empty();
      return new PartitionDrainResult(false, state.completedCount, abandoned, committable);
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public void clearPartition(TopicPartition tp) {
    PartitionState state = partitions.remove(tp);
    if (state != null) {
      state.lock.lock();
      try {
        if (state.currentInProgress != -1 || state.pendingCount > 0) {
          System.getLogger(SequentialOffsetTracker.class.getName())
              .log(
                  System.Logger.Level.WARNING,
                  "Clearing partition {0} with {1} pending and in-progress={2}. "
                      + "These records will be re-consumed by the new owner.",
                  tp,
                  state.pendingCount,
                  state.currentInProgress);
        }
      } finally {
        state.lock.unlock();
      }
    }
  }

  @Override
  public int pendingCount(TopicPartition tp) {
    PartitionState state = partitions.get(tp);
    if (state == null) return 0;
    state.lock.lock();
    try {
      return state.pendingCount;
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public int inProgressCount(TopicPartition tp) {
    PartitionState state = partitions.get(tp);
    if (state == null) return 0;
    state.lock.lock();
    try {
      return state.currentInProgress != -1 ? 1 : 0;
    } finally {
      state.lock.unlock();
    }
  }

  @Override
  public long lag(TopicPartition tp) {
    PartitionState state = partitions.get(tp);
    if (state == null) return 0;
    state.lock.lock();
    try {
      if (state.highestRegistered < 0) return 0;
      return state.highestRegistered + 1 - state.committableOffset;
    } finally {
      state.lock.unlock();
    }
  }

  private PartitionState getOrThrow(TopicPartition tp) {
    PartitionState state = partitions.get(tp);
    if (state == null) {
      throw new IllegalStateException(
          "Partition " + tp + " not initialized. Call initPartition first.");
    }
    return state;
  }

  private void validateNotFailed(PartitionState state, TopicPartition tp) {
    if (state.failed) {
      throw new IllegalStateException("Partition " + tp + " is in failed state");
    }
  }
}
