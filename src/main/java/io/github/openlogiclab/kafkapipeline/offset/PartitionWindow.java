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
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sliding window that tracks per-offset completion status for a single partition.
 *
 * <p>Supports out-of-order acks: records can complete in any order, but {@link
 * #getCommittableOffset()} only advances past contiguous DONE entries (like TCP's sliding window —
 * only the left edge moves forward).
 *
 * <p>Thread-safe with fine-grained locking: uses {@link ConcurrentSkipListMap} for lock-free entry
 * access, atomic counters for lock-free monitoring, and a dedicated lock only for window shrinking
 * and drain synchronization.
 */
final class PartitionWindow {

  private final ReentrantLock registerLock = new ReentrantLock();
  private final ReentrantLock shrinkLock = new ReentrantLock();
  private final Condition drained = shrinkLock.newCondition();

  static final int DEFAULT_MAX_WINDOW_SIZE = 10_000;

  private final long baseOffset;
  private final int maxWindowSize;
  private final AtomicLong committableOffset;
  private final AtomicLong highestRegistered = new AtomicLong(-1);
  private final ConcurrentSkipListMap<Long, OffsetStatus> entries = new ConcurrentSkipListMap<>();

  private final AtomicInteger pendingCount = new AtomicInteger(0);
  private final AtomicInteger inProgressCount = new AtomicInteger(0);
  private final AtomicInteger completedCount = new AtomicInteger(0);
  private final AtomicBoolean failed = new AtomicBoolean(false);

  PartitionWindow(long startOffset) {
    this(startOffset, DEFAULT_MAX_WINDOW_SIZE);
  }

  PartitionWindow(long startOffset, int maxWindowSize) {
    if (maxWindowSize <= 0) {
      throw new IllegalArgumentException("maxWindowSize must be positive, got " + maxWindowSize);
    }
    this.baseOffset = startOffset;
    this.committableOffset = new AtomicLong(startOffset);
    this.maxWindowSize = maxWindowSize;
  }

  void register(long offset) {
    registerLock.lock();
    try {
      validateNotFailed();
      validateWindowCapacity(1);
      if (entries.containsKey(offset)) {
        throw new IllegalStateException("Offset " + offset + " already tracked in window");
      }
      entries.put(offset, OffsetStatus.REGISTERED);
      updateHighestRegistered(offset);
      pendingCount.incrementAndGet();
    } finally {
      registerLock.unlock();
    }
  }

  void registerBatch(long[] offsets) {
    registerLock.lock();
    try {
      validateNotFailed();
      validateWindowCapacity(offsets.length);
      for (int i = 0; i < offsets.length; i++) {
        long offset = offsets[i];
        if (entries.containsKey(offset)) {
          rollbackBatch(offsets, i);
          throw new IllegalStateException("Offset " + offset + " already tracked in window");
        }
        entries.put(offset, OffsetStatus.REGISTERED);
        updateHighestRegistered(offset);
        pendingCount.incrementAndGet();
      }
    } finally {
      registerLock.unlock();
    }
  }

  void markInProgress(long offset) {
    validateNotFailed();
    OffsetStatus prev =
        entries.computeIfPresent(
            offset,
            (k, status) -> {
              if (status != OffsetStatus.REGISTERED) {
                throw new IllegalStateException(
                    "Cannot mark offset " + offset + " in-progress, current status: " + status);
              }
              return OffsetStatus.IN_PROGRESS;
            });
    if (prev == null) {
      throw new IllegalStateException(
          "Cannot mark offset " + offset + " in-progress, current status: null");
    }
    pendingCount.decrementAndGet();
    inProgressCount.incrementAndGet();
  }

  void markBatchInProgress(long[] offsets) {
    validateNotFailed();
    for (long offset : offsets) {
      markInProgress(offset);
    }
  }

  void ack(long offset) {
    OffsetStatus prev =
        entries.computeIfPresent(
            offset,
            (k, status) -> {
              if (status != OffsetStatus.IN_PROGRESS) {
                throw new IllegalStateException(
                    "Cannot ack offset " + offset + ", current status: " + status);
              }
              return OffsetStatus.DONE;
            });
    if (prev == null) {
      throw new IllegalStateException("Cannot ack offset " + offset + ", current status: null");
    }
    inProgressCount.decrementAndGet();
    completedCount.incrementAndGet();

    tryShrinkAndSignal();
  }

  void ackBatch(long[] offsets) {
    for (long offset : offsets) {
      OffsetStatus prev =
          entries.computeIfPresent(
              offset,
              (k, status) -> {
                if (status != OffsetStatus.IN_PROGRESS) {
                  throw new IllegalStateException(
                      "Cannot ack offset " + offset + ", current status: " + status);
                }
                return OffsetStatus.DONE;
              });
      if (prev == null) {
        throw new IllegalStateException("Cannot ack offset " + offset + ", current status: null");
      }
      inProgressCount.decrementAndGet();
      completedCount.incrementAndGet();
    }
    tryShrinkAndSignal();
  }

  void fail(long offset) {
    OffsetStatus prev =
        entries.computeIfPresent(
            offset,
            (k, status) -> {
              if (status != OffsetStatus.IN_PROGRESS) {
                throw new IllegalStateException(
                    "Cannot fail offset " + offset + ", current status: " + status);
              }
              return OffsetStatus.FAILED;
            });
    if (prev == null) {
      throw new IllegalStateException("Cannot fail offset " + offset + ", current status: null");
    }
    inProgressCount.decrementAndGet();
    failed.set(true);
    signalDrained();
  }

  void failBatch(long[] offsets) {
    for (long offset : offsets) {
      OffsetStatus prev =
          entries.computeIfPresent(
              offset,
              (k, status) -> {
                if (status != OffsetStatus.IN_PROGRESS) {
                  throw new IllegalStateException(
                      "Cannot fail offset " + offset + ", current status: " + status);
                }
                return OffsetStatus.FAILED;
              });
      if (prev == null) {
        throw new IllegalStateException("Cannot fail offset " + offset + ", current status: null");
      }
      inProgressCount.decrementAndGet();
    }
    failed.set(true);
    signalDrained();
  }

  OptionalLong getCommittableOffset() {
    long current = committableOffset.get();
    return current > baseOffset ? OptionalLong.of(current) : OptionalLong.empty();
  }

  PartitionDrainResult drain(Duration timeout) {
    shrinkLock.lock();
    try {
      long deadline = System.nanoTime() + timeout.toNanos();
      while (inProgressCount.get() > 0 && !failed.get()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        drained.await(remaining, TimeUnit.NANOSECONDS);
      }
      shrinkWindow();
      boolean allCompleted = inProgressCount.get() == 0 && pendingCount.get() == 0 && !failed.get();
      int abandoned = pendingCount.get() + inProgressCount.get();
      long current = committableOffset.get();
      OptionalLong committable =
          current > baseOffset ? OptionalLong.of(current) : OptionalLong.empty();
      return new PartitionDrainResult(allCompleted, completedCount.get(), abandoned, committable);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      int abandoned = pendingCount.get() + inProgressCount.get();
      long current = committableOffset.get();
      OptionalLong committable =
          current > baseOffset ? OptionalLong.of(current) : OptionalLong.empty();
      return new PartitionDrainResult(false, completedCount.get(), abandoned, committable);
    } finally {
      shrinkLock.unlock();
    }
  }

  int pendingCount() {
    return pendingCount.get();
  }

  int inProgressCount() {
    return inProgressCount.get();
  }

  int windowSize() {
    return entries.size();
  }

  boolean isFull() {
    return entries.size() >= maxWindowSize;
  }

  long lag() {
    long highest = highestRegistered.get();
    if (highest < 0) return 0;
    return highest + 1 - committableOffset.get();
  }

  boolean isFailed() {
    return failed.get();
  }

  void resolveFailure(long offset) {
    OffsetStatus prev =
        entries.computeIfPresent(
            offset,
            (k, status) -> {
              if (status != OffsetStatus.FAILED) {
                throw new IllegalStateException(
                    "Cannot resolve offset " + offset + ", current status: " + status);
              }
              return OffsetStatus.DONE;
            });
    if (prev == null) {
      throw new IllegalStateException("Cannot resolve offset " + offset + ", current status: null");
    }
    completedCount.incrementAndGet();
    failed.set(false);
    tryShrinkAndSignal();
  }

  void resolveBatchFailure(long[] offsets) {
    for (long offset : offsets) {
      OffsetStatus prev =
          entries.computeIfPresent(
              offset,
              (k, status) -> {
                if (status != OffsetStatus.FAILED) {
                  throw new IllegalStateException(
                      "Cannot resolve offset " + offset + ", current status: " + status);
                }
                return OffsetStatus.DONE;
              });
      if (prev == null) {
        throw new IllegalStateException(
            "Cannot resolve offset " + offset + ", current status: null");
      }
      completedCount.incrementAndGet();
    }
    failed.set(false);
    tryShrinkAndSignal();
  }

  private void tryShrinkAndSignal() {
    if (shrinkLock.tryLock()) {
      try {
        shrinkWindow();
        drained.signalAll();
      } finally {
        shrinkLock.unlock();
      }
    }
  }

  private void signalDrained() {
    shrinkLock.lock();
    try {
      drained.signalAll();
    } finally {
      shrinkLock.unlock();
    }
  }

  private void shrinkWindow() {
    Map.Entry<Long, OffsetStatus> first;
    while ((first = entries.firstEntry()) != null && first.getValue() == OffsetStatus.DONE) {
      if (entries.remove(first.getKey(), OffsetStatus.DONE)) {
        committableOffset.set(first.getKey() + 1);
      } else {
        break;
      }
    }
  }

  private void updateHighestRegistered(long offset) {
    long current;
    do {
      current = highestRegistered.get();
      if (offset <= current) return;
    } while (!highestRegistered.compareAndSet(current, offset));
  }

  private void rollbackBatch(long[] offsets, int failedAtIndex) {
    for (int i = 0; i < failedAtIndex; i++) {
      entries.remove(offsets[i]);
      pendingCount.decrementAndGet();
    }
  }

  private void validateNotFailed() {
    if (failed.get()) {
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
