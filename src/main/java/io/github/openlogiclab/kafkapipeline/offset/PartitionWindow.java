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
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sliding window that tracks per-offset completion status for a single partition.
 *
 * <p>Supports out-of-order acks: records can complete in any order, but {@link
 * #getCommittableOffset()} only advances past contiguous DONE entries (like TCP's sliding window —
 * only the left edge moves forward).
 *
 * <p>Thread-safe: all public methods acquire the internal lock.
 */
final class PartitionWindow {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition drained = lock.newCondition();

  static final int DEFAULT_MAX_WINDOW_SIZE = 10_000;

  private final long baseOffset;
  private final int maxWindowSize;
  private long committableOffset;
  private long highestRegistered = -1;
  private final TreeMap<Long, OffsetStatus> entries = new TreeMap<>();

  private int pendingCount = 0;
  private int inProgressCount = 0;
  private int completedCount = 0;
  private boolean failed = false;

  PartitionWindow(long startOffset) {
    this(startOffset, DEFAULT_MAX_WINDOW_SIZE);
  }

  PartitionWindow(long startOffset, int maxWindowSize) {
    if (maxWindowSize <= 0) {
      throw new IllegalArgumentException("maxWindowSize must be positive, got " + maxWindowSize);
    }
    this.baseOffset = startOffset;
    this.committableOffset = startOffset;
    this.maxWindowSize = maxWindowSize;
  }

  void register(long offset) {
    lock.lock();
    try {
      validateNotFailed();
      validateWindowCapacity(1);
      if (entries.containsKey(offset)) {
        throw new IllegalStateException("Offset " + offset + " already tracked in window");
      }
      entries.put(offset, OffsetStatus.REGISTERED);
      highestRegistered = Math.max(highestRegistered, offset);
      pendingCount++;
    } finally {
      lock.unlock();
    }
  }

  void registerBatch(long fromOffset, long toOffset) {
    lock.lock();
    try {
      validateNotFailed();
      int batchSize = (int) (toOffset - fromOffset + 1);
      validateWindowCapacity(batchSize);
      for (long offset = fromOffset; offset <= toOffset; offset++) {
        if (entries.containsKey(offset)) {
          rollbackBatch(fromOffset, offset);
          throw new IllegalStateException("Offset " + offset + " already tracked in window");
        }
        entries.put(offset, OffsetStatus.REGISTERED);
        pendingCount++;
      }
      highestRegistered = Math.max(highestRegistered, toOffset);
    } finally {
      lock.unlock();
    }
  }

  void markInProgress(long offset) {
    lock.lock();
    try {
      validateNotFailed();
      OffsetStatus status = entries.get(offset);
      if (status != OffsetStatus.REGISTERED) {
        throw new IllegalStateException(
            "Cannot mark offset " + offset + " in-progress, current status: " + status);
      }
      entries.put(offset, OffsetStatus.IN_PROGRESS);
      pendingCount--;
      inProgressCount++;
    } finally {
      lock.unlock();
    }
  }

  void markBatchInProgress(long fromOffset, long toOffset) {
    lock.lock();
    try {
      validateNotFailed();
      for (long offset = fromOffset; offset <= toOffset; offset++) {
        OffsetStatus status = entries.get(offset);
        if (status != OffsetStatus.REGISTERED) {
          throw new IllegalStateException(
              "Cannot mark offset " + offset + " in-progress, current status: " + status);
        }
        entries.put(offset, OffsetStatus.IN_PROGRESS);
        pendingCount--;
        inProgressCount++;
      }
    } finally {
      lock.unlock();
    }
  }

  void ack(long offset) {
    lock.lock();
    try {
      OffsetStatus status = entries.get(offset);
      if (status != OffsetStatus.IN_PROGRESS) {
        throw new IllegalStateException(
            "Cannot ack offset " + offset + ", current status: " + status);
      }
      entries.put(offset, OffsetStatus.DONE);
      inProgressCount--;
      completedCount++;

      if (!entries.isEmpty() && entries.firstKey() == offset) {
        shrinkWindow();
      }

      drained.signalAll();
    } finally {
      lock.unlock();
    }
  }

  void ackBatch(long fromOffset, long toOffset) {
    lock.lock();
    try {
      for (long offset = fromOffset; offset <= toOffset; offset++) {
        OffsetStatus status = entries.get(offset);
        if (status != OffsetStatus.IN_PROGRESS) {
          throw new IllegalStateException(
              "Cannot ack offset " + offset + ", current status: " + status);
        }
        entries.put(offset, OffsetStatus.DONE);
        inProgressCount--;
        completedCount++;
      }
      shrinkWindow();
      drained.signalAll();
    } finally {
      lock.unlock();
    }
  }

  void fail(long offset) {
    lock.lock();
    try {
      OffsetStatus status = entries.get(offset);
      if (status != OffsetStatus.IN_PROGRESS) {
        throw new IllegalStateException(
            "Cannot fail offset " + offset + ", current status: " + status);
      }
      entries.put(offset, OffsetStatus.FAILED);
      inProgressCount--;
      failed = true;
      drained.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Scans the window from the left edge, advancing past contiguous DONE entries. Completed entries
   * are removed to keep the window compact.
   *
   * @return the offset safe to commit (Kafka semantics: "start from here next time"), or empty if
   *     no progress since partition init
   */
  OptionalLong getCommittableOffset() {
    lock.lock();
    try {
      shrinkWindow();
      return committableOffset > baseOffset
          ? OptionalLong.of(committableOffset)
          : OptionalLong.empty();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Waits for all in-progress records to complete (or timeout). Pending (REGISTERED) records that
   * were never picked up are counted as abandoned.
   */
  PartitionDrainResult drain(Duration timeout) {
    lock.lock();
    try {
      long deadline = System.nanoTime() + timeout.toNanos();
      while (inProgressCount > 0 && !failed) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        drained.await(remaining, TimeUnit.NANOSECONDS);
      }
      shrinkWindow();
      boolean allCompleted = inProgressCount == 0 && pendingCount == 0 && !failed;
      int abandoned = pendingCount + inProgressCount;
      OptionalLong committable =
          committableOffset > baseOffset
              ? OptionalLong.of(committableOffset)
              : OptionalLong.empty();
      return new PartitionDrainResult(allCompleted, completedCount, abandoned, committable);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      int abandoned = pendingCount + inProgressCount;
      OptionalLong committable =
          committableOffset > baseOffset
              ? OptionalLong.of(committableOffset)
              : OptionalLong.empty();
      return new PartitionDrainResult(false, completedCount, abandoned, committable);
    } finally {
      lock.unlock();
    }
  }

  int pendingCount() {
    lock.lock();
    try {
      return pendingCount;
    } finally {
      lock.unlock();
    }
  }

  int inProgressCount() {
    lock.lock();
    try {
      return inProgressCount;
    } finally {
      lock.unlock();
    }
  }

  int windowSize() {
    lock.lock();
    try {
      return entries.size();
    } finally {
      lock.unlock();
    }
  }

  boolean isFull() {
    lock.lock();
    try {
      return entries.size() >= maxWindowSize;
    } finally {
      lock.unlock();
    }
  }

  long lag() {
    lock.lock();
    try {
      if (highestRegistered < 0) return 0;
      return highestRegistered + 1 - committableOffset;
    } finally {
      lock.unlock();
    }
  }

  boolean isFailed() {
    lock.lock();
    try {
      return failed;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Resolves a FAILED offset by converting it to DONE, allowing the window to advance past it.
   * Clears the failed state so new records can be accepted.
   *
   * @throws IllegalStateException if the offset is not in FAILED state
   */
  void resolveFailure(long offset) {
    lock.lock();
    try {
      OffsetStatus status = entries.get(offset);
      if (status != OffsetStatus.FAILED) {
        throw new IllegalStateException(
            "Cannot resolve offset " + offset + ", current status: " + status);
      }
      entries.put(offset, OffsetStatus.DONE);
      completedCount++;
      failed = false;

      if (!entries.isEmpty() && entries.firstKey() == offset) {
        shrinkWindow();
      }

      drained.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Advances {@code committableOffset} past contiguous DONE entries at the left edge of the window,
   * removing them as it goes. Must be called while holding the lock.
   */
  private void shrinkWindow() {
    Iterator<Map.Entry<Long, OffsetStatus>> it = entries.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, OffsetStatus> entry = it.next();
      if (entry.getValue() != OffsetStatus.DONE) {
        break;
      }
      committableOffset = entry.getKey() + 1;
      it.remove();
    }
  }

  private void rollbackBatch(long fromOffset, long failedAt) {
    for (long offset = fromOffset; offset < failedAt; offset++) {
      entries.remove(offset);
      pendingCount--;
    }
  }

  private void validateNotFailed() {
    if (failed) {
      throw new IllegalStateException("Partition window is in failed state");
    }
  }

  private void validateWindowCapacity(int count) {
    if (entries.size() + count > maxWindowSize) {
      throw new IllegalStateException(
          "Window full: current="
              + entries.size()
              + " + requested="
              + count
              + " exceeds max="
              + maxWindowSize
              + ". Apply backpressure (pause partition).");
    }
  }
}
