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
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;

/**
 * Offset tracker for unordered processing (Mode B).
 *
 * <p>Records within a partition can be processed concurrently and acked in any order. Uses a
 * per-partition {@link PartitionWindow} (sliding window) to track individual offset states. The
 * committable offset only advances past contiguous completed entries, preventing data loss on
 * crash/rebalance.
 */
public final class UnorderedOffsetTracker implements OffsetTracker {

  private final ConcurrentHashMap<TopicPartition, PartitionWindow> partitions =
      new ConcurrentHashMap<>();

  @Override
  public void register(TopicPartition tp, long offset) {
    getOrThrow(tp).register(offset);
  }

  @Override
  public void registerBatch(TopicPartition tp, long fromOffset, long toOffset) {
    if (fromOffset > toOffset) {
      throw new IllegalArgumentException("fromOffset " + fromOffset + " > toOffset " + toOffset);
    }
    getOrThrow(tp).registerBatch(fromOffset, toOffset);
  }

  @Override
  public void markInProgress(TopicPartition tp, long offset) {
    getOrThrow(tp).markInProgress(offset);
  }

  @Override
  public void markBatchInProgress(TopicPartition tp, long fromOffset, long toOffset) {
    getOrThrow(tp).markBatchInProgress(fromOffset, toOffset);
  }

  @Override
  public void ack(TopicPartition tp, long offset) {
    getOrThrow(tp).ack(offset);
  }

  @Override
  public void ackBatch(TopicPartition tp, long fromOffset, long toOffset) {
    getOrThrow(tp).ackBatch(fromOffset, toOffset);
  }

  @Override
  public void fail(TopicPartition tp, long offset) {
    getOrThrow(tp).fail(offset);
  }

  @Override
  public void resolveFailure(TopicPartition tp, long offset) {
    getOrThrow(tp).resolveFailure(offset);
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
  public Map<TopicPartition, Long> getAllCommittableOffsets() {
    Map<TopicPartition, Long> result = new HashMap<>();
    for (Map.Entry<TopicPartition, PartitionWindow> entry : partitions.entrySet()) {
      entry
          .getValue()
          .getCommittableOffset()
          .ifPresent(offset -> result.put(entry.getKey(), offset));
    }
    return result;
  }

  @Override
  public void initPartition(TopicPartition tp, long startOffset) {
    PartitionWindow existing = partitions.putIfAbsent(tp, new PartitionWindow(startOffset));
    if (existing != null) {
      throw new IllegalStateException(
          "Partition " + tp + " already initialized. Call clearPartition first.");
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

  private static final System.Logger logger =
      System.getLogger(UnorderedOffsetTracker.class.getName());

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

  private PartitionWindow getOrThrow(TopicPartition tp) {
    PartitionWindow window = partitions.get(tp);
    if (window == null) {
      throw new IllegalStateException(
          "Partition " + tp + " not initialized. Call initPartition first.");
    }
    return window;
  }
}
