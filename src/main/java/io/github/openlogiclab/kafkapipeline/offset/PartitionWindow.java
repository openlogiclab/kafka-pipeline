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
import java.util.Map;
import java.util.Optional;
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
  private final AtomicBoolean shrinkRequested = new AtomicBoolean();
  // Bumped whenever the commit point may have changed (an offset finished, tracking stopped), so
  // the commit tick can skip rebuilding and re-encoding an unchanged commit point.
  private final AtomicLong version = new AtomicLong();
  private long cachedVersion = -1; // guarded by shrinkLock
  private CommitPoint cachedPoint; // guarded by shrinkLock

  static final int DEFAULT_MAX_WINDOW_SIZE = 10_000;

  private final long baseOffset;
  private final long generation;
  private final int maxWindowSize;
  private final AtomicLong committableOffset;
  private final AtomicLong highestRegistered = new AtomicLong(-1);
  private final ConcurrentSkipListMap<Long, OffsetStatus> entries = new ConcurrentSkipListMap<>();

  private final AtomicLong committedOffset; // Last offset confirmed committed to Kafka

  // entries.size() is O(n) on ConcurrentSkipListMap, so track the size separately.
  private final AtomicInteger windowSize = new AtomicInteger(0);
  private final AtomicInteger pendingCount = new AtomicInteger(0);
  private final AtomicInteger inProgressCount = new AtomicInteger(0);
  private final AtomicInteger completedCount = new AtomicInteger(0);
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private volatile boolean failed = false;
  // When the committable offset last advanced, or the window last went from empty to non-empty.
  private volatile long lastAdvanceNanos = System.nanoTime();

  // Crash replay: offsets the previous owner finished beyond its committed offset (relative to
  // baseOffset, read-only after construction), and whether to encode our own finished offsets.
  private volatile boolean trackCompleted;
  private final BitSet preDone;
  private final Object commitMonitor = new Object();
  private String committedMetadata; // guarded by commitMonitor

  PartitionWindow(long startOffset) {
    this(startOffset, DEFAULT_MAX_WINDOW_SIZE);
  }

  PartitionWindow(long startOffset, int maxWindowSize) {
    this(startOffset, maxWindowSize, OffsetTracker.NO_GENERATION);
  }

  PartitionWindow(long startOffset, int maxWindowSize, long generation) {
    this(startOffset, maxWindowSize, generation, false, new BitSet());
  }

  /**
   * Creates a window.
   *
   * @param startOffset first offset this consumer will fetch
   * @param maxWindowSize capacity
   * @param generation rebalance generation
   * @param trackCompleted whether commit points carry the finished offsets beyond the committed one
   * @param preDone offsets (relative to {@code startOffset}) that a previous owner already
   *     finished; they are registered as DONE and never dispatched
   */
  PartitionWindow(
      long startOffset,
      int maxWindowSize,
      long generation,
      boolean trackCompleted,
      BitSet preDone) {
    if (maxWindowSize <= 0) {
      throw new IllegalArgumentException("maxWindowSize must be positive, got " + maxWindowSize);
    }
    this.baseOffset = startOffset;
    this.generation = generation;
    this.committableOffset = new AtomicLong(startOffset);
    this.committedOffset = new AtomicLong(startOffset);
    this.maxWindowSize = maxWindowSize;
    this.trackCompleted = trackCompleted;
    this.preDone = trackCompleted ? (BitSet) preDone.clone() : new BitSet();
    this.committedMetadata = CompletedOffsetsCodec.encode(startOffset, this.preDone);
  }

  /** Stops recording finished offsets in commit points; later points carry no metadata. */
  void stopTrackingCompleted() {
    trackCompleted = false;
    version.incrementAndGet();
  }

  /** True if a previous owner already finished this offset, so it must not be processed again. */
  boolean isPreDone(long offset) {
    long rel = offset - baseOffset;
    // preDone.length() is at most CompletedOffsetsCodec.MAX_SPAN, so the cast is safe.
    return rel > 0 && rel < preDone.length() && preDone.get((int) rel);
  }

  /**
   * Registers an offset, or records it as DONE right away if a previous owner already finished it.
   *
   * @return true if the record must be processed, false if it was already done
   */
  boolean registerUnlessDone(long offset) {
    if (!isPreDone(offset)) {
      register(offset);
      return true;
    }
    registerLock.lock();
    try {
      validateWindowCapacity(1);
      if (entries.containsKey(offset)) {
        throw new IllegalStateException("Offset " + offset + " already tracked in window");
      }
      entries.put(offset, OffsetStatus.DONE);
      windowSize.incrementAndGet();
      updateHighestRegistered(offset);
      completedCount.incrementAndGet();
      version.incrementAndGet();
    } finally {
      registerLock.unlock();
    }
    tryShrink();
    return false;
  }

  /**
   * Registers a batch, recording offsets a previous owner already finished as DONE.
   *
   * @return the offsets that must be processed, in order (empty if all were already done)
   */
  long[] registerBatchUnlessDone(long[] offsets) {
    int skip = 0;
    for (long offset : offsets) {
      if (isPreDone(offset)) skip++;
    }
    if (skip == 0) {
      registerBatch(offsets);
      return offsets;
    }
    long[] todo = new long[offsets.length - skip];
    registerLock.lock();
    try {
      validateWindowCapacity(offsets.length);
      for (long offset : offsets) {
        if (entries.containsKey(offset)) {
          throw new IllegalStateException("Offset " + offset + " already tracked in window");
        }
      }
      if (windowSize.get() == 0) lastAdvanceNanos = System.nanoTime();
      int t = 0;
      for (long offset : offsets) {
        boolean done = isPreDone(offset);
        entries.put(offset, done ? OffsetStatus.DONE : OffsetStatus.REGISTERED);
        windowSize.incrementAndGet();
        updateHighestRegistered(offset);
        if (done) {
          completedCount.incrementAndGet();
          version.incrementAndGet();
        } else {
          pendingCount.incrementAndGet();
          todo[t++] = offset;
        }
      }
    } finally {
      registerLock.unlock();
    }
    tryShrink();
    return todo;
  }

  void register(long offset) {
    registerLock.lock();
    try {
      validateWindowCapacity(1);
      if (entries.containsKey(offset)) {
        throw new IllegalStateException("Offset " + offset + " already tracked in window");
      }
      entries.put(offset, OffsetStatus.REGISTERED);
      if (windowSize.getAndIncrement() == 0) lastAdvanceNanos = System.nanoTime();
      updateHighestRegistered(offset);
      pendingCount.incrementAndGet();
    } finally {
      registerLock.unlock();
    }
  }

  void registerBatch(long[] offsets) {
    registerLock.lock();
    try {
      validateWindowCapacity(offsets.length);
      if (windowSize.get() == 0) lastAdvanceNanos = System.nanoTime();
      for (int i = 0; i < offsets.length; i++) {
        long offset = offsets[i];
        if (entries.containsKey(offset)) {
          rollbackBatch(offsets, i);
          throw new IllegalStateException("Offset " + offset + " already tracked in window");
        }
        entries.put(offset, OffsetStatus.REGISTERED);
        windowSize.incrementAndGet();
        updateHighestRegistered(offset);
        pendingCount.incrementAndGet();
      }
    } finally {
      registerLock.unlock();
    }
  }

  long generation() {
    return generation;
  }

  void markInProgress(long offset) {
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

  /**
   * Marks a batch of offsets as IN_PROGRESS. Lenient: skips offsets not in REGISTERED state.
   *
   * @return number of offsets actually transitioned
   */
  int markBatchInProgress(long[] offsets) {
    int transitioned = 0;
    for (long offset : offsets) {
      boolean[] changed = {false};
      entries.computeIfPresent(
          offset,
          (k, status) -> {
            if (status == OffsetStatus.REGISTERED) {
              changed[0] = true;
              return OffsetStatus.IN_PROGRESS;
            }
            return status;
          });
      if (changed[0]) {
        transitioned++;
      }
    }
    if (transitioned > 0) {
      pendingCount.addAndGet(-transitioned);
      inProgressCount.addAndGet(transitioned);
    }
    return transitioned;
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
    int remaining = inProgressCount.decrementAndGet();
    completedCount.incrementAndGet();
    version.incrementAndGet();

    tryShrink();
    if (remaining == 0) {
      signalDrained();
    }
  }

  /**
   * Acks a batch of offsets. Lenient: skips offsets not in IN_PROGRESS state.
   *
   * @return number of offsets actually acked
   */
  int ackBatch(long[] offsets) {
    int transitioned = 0;
    for (long offset : offsets) {
      boolean[] changed = {false};
      entries.computeIfPresent(
          offset,
          (k, status) -> {
            if (status == OffsetStatus.IN_PROGRESS) {
              changed[0] = true;
              return OffsetStatus.DONE;
            }
            return status;
          });
      if (changed[0]) {
        transitioned++;
      }
    }
    if (transitioned > 0) {
      int remaining = inProgressCount.addAndGet(-transitioned);
      completedCount.addAndGet(transitioned);
      version.incrementAndGet();
      tryShrink();
      if (remaining == 0) {
        signalDrained();
      }
    }
    return transitioned;
  }

  /**
   * Returns the committable offset if it has advanced since the last confirmed commit. Used for
   * periodic commits to avoid redundant commits when nothing changed.
   */
  OptionalLong getUncommittedOffset() {
    shrinkLock.lock();
    try {
      shrinkWindow();
      long current = committableOffset.get();
      long committed = committedOffset.get();
      if (current > committed) {
        return OptionalLong.of(current);
      }
      return OptionalLong.empty();
    } finally {
      unlockShrink();
    }
  }

  /**
   * Returns the committable offset regardless of whether it was already committed. Used for drain
   * and shutdown where we always want to commit the final position.
   */
  OptionalLong getCommittableOffset() {
    shrinkLock.lock();
    try {
      shrinkWindow();
      long current = committableOffset.get();
      return current > baseOffset ? OptionalLong.of(current) : OptionalLong.empty();
    } finally {
      unlockShrink();
    }
  }

  /**
   * Returns the commit point if it changed since the last confirmed commit: the committable offset
   * advanced, or (when tracking completed offsets) the set of finished offsets beyond it changed.
   */
  Optional<CommitPoint> getUncommittedCommitPoint() {
    CommitPoint point = commitPoint();
    synchronized (commitMonitor) {
      long committed = committedOffset.get();
      if (point.offset() > committed
          || (point.offset() == committed && !point.metadata().equals(committedMetadata))) {
        return Optional.of(point);
      }
      return Optional.empty();
    }
  }

  /**
   * Returns the current commit point, or empty if there is nothing worth committing (no progress
   * past the start offset and no finished offsets to record).
   */
  Optional<CommitPoint> getCommitPoint() {
    CommitPoint point = commitPoint();
    if (point.offset() > baseOffset || !point.metadata().isEmpty()) {
      return Optional.of(point);
    }
    return Optional.empty();
  }

  /** Records that this commit point was confirmed committed to Kafka. */
  void markCommitted(CommitPoint point) {
    synchronized (commitMonitor) {
      long committed = committedOffset.get();
      if (point.offset() > committed) {
        committedOffset.set(point.offset());
        committedMetadata = point.metadata();
      } else if (point.offset() == committed) {
        committedMetadata = point.metadata();
      }
    }
  }

  /**
   * Builds the offset to commit and the finished offsets beyond it from one consistent view: the
   * committable offset cannot move while the shrink lock is held.
   */
  private CommitPoint commitPoint() {
    shrinkLock.lock();
    try {
      // Always shrink, even when the cached point is reused: the commit tick is also the backstop
      // that keeps windowSize (and so the window pause) accurate.
      shrinkWindow();
      long c = committableOffset.get();
      if (!trackCompleted) {
        return new CommitPoint(c, "");
      }
      // Read the version before building, so a change made meanwhile forces a rebuild next time.
      long v = version.get();
      if (v == cachedVersion && cachedPoint != null && cachedPoint.offset() >= c) {
        return cachedPoint;
      }
      CommitPoint point = buildCommitPoint(c);
      cachedVersion = v;
      cachedPoint = point;
      return point;
    } finally {
      unlockShrink();
    }
  }

  /** Builds the commit point from offset {@code c}; caller holds the shrink lock. */
  private CommitPoint buildCommitPoint(long c) {
    // Records the previous owner finished right at the committable offset need not be fetched
    // again before the commit can move past them.
    while (isPreDone(c)) {
      c++;
    }
    BitSet done = new BitSet();
    for (Map.Entry<Long, OffsetStatus> e : entries.tailMap(c, false).entrySet()) {
      if (e.getValue() == OffsetStatus.DONE) {
        long rel = e.getKey() - c;
        if (rel >= CompletedOffsetsCodec.MAX_SPAN) break; // sorted: the rest are further
        done.set((int) rel);
      }
    }
    // Offsets the previous owner finished that we have not fetched again yet are still done.
    long from = Math.max(1, c - baseOffset + 1);
    if (from < preDone.length()) {
      for (int i = preDone.nextSetBit((int) from); i >= 0; i = preDone.nextSetBit(i + 1)) {
        // c >= baseOffset, so rel <= i < MAX_SPAN: the decoder never yields more bits.
        done.set((int) (baseOffset + i - c));
      }
    }
    // Only rebuilt when the window changed: at most one window of entries plus a small deflate.
    return new CommitPoint(c, CompletedOffsetsCodec.encode(c, done));
  }

  /**
   * Marks an offset as confirmed committed to Kafka. Called from async commit callback. Uses max()
   * to handle out-of-order callbacks safely.
   */
  void markCommitted(long offset) {
    markCommitted(new CommitPoint(offset, ""));
  }

  PartitionDrainResult drain(Duration timeout) {
    shrinkLock.lock();
    try {
      long deadline = System.nanoTime() + timeout.toNanos();
      while (inProgressCount.get() > 0) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        drained.await(remaining, TimeUnit.NANOSECONDS);
      }
      shrinkWindow();
      boolean allCompleted = inProgressCount.get() == 0 && pendingCount.get() == 0;
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
      unlockShrink();
    }
  }

  int pendingCount() {
    return pendingCount.get();
  }

  int inProgressCount() {
    return inProgressCount.get();
  }

  int windowSize() {
    return windowSize.get();
  }

  boolean isFull() {
    return windowSize.get() >= maxWindowSize;
  }

  /** Returns true if any unrecoverable failure has occurred on this partition. */
  boolean isFailed() {
    return failed;
  }

  /** Records that an unrecoverable failure occurred. Called after FinalFailureHandler. */
  void markFailed() {
    failed = true;
    failureCount.incrementAndGet();
  }

  /** Number of unrecoverable failures that have occurred on this partition. */
  int failureCount() {
    return failureCount.get();
  }

  /** Milliseconds the committable offset has not moved while records wait; 0 if none wait. */
  long commitStallMillis() {
    if (pendingCount.get() + inProgressCount.get() <= 0) return 0;
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastAdvanceNanos);
  }

  long lag() {
    long highest = highestRegistered.get();
    if (highest < 0) return 0;
    return highest + 1 - committableOffset.get();
  }

  /**
   * Shrinks without blocking. If another thread holds the lock, leaves a request that the holder
   * serves when it unlocks ({@link #unlockShrink()}), so an ack that lands while another thread is
   * shrinking is never left unshrunk at the head of the window until the next commit tick.
   */
  private void tryShrink() {
    shrinkRequested.set(true);
    while (shrinkRequested.get() && shrinkLock.tryLock()) {
      try {
        shrinkRequested.set(false);
        shrinkWindow();
      } finally {
        shrinkLock.unlock();
      }
    }
  }

  /** Unlocks the shrink lock, then serves a shrink requested while it was held. */
  private void unlockShrink() {
    shrinkLock.unlock();
    if (shrinkRequested.get()) tryShrink();
  }

  /** Signals waiting drain threads. Called when inProgressCount reaches 0. */
  private void signalDrained() {
    shrinkLock.lock();
    try {
      drained.signalAll();
    } finally {
      unlockShrink();
    }
  }

  private void shrinkWindow() {
    Map.Entry<Long, OffsetStatus> first;
    while ((first = entries.firstEntry()) != null && first.getValue() == OffsetStatus.DONE) {
      if (entries.remove(first.getKey(), OffsetStatus.DONE)) {
        windowSize.decrementAndGet();
        committableOffset.set(first.getKey() + 1);
        lastAdvanceNanos = System.nanoTime();
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
      windowSize.decrementAndGet();
      pendingCount.decrementAndGet();
    }
  }

  private void validateWindowCapacity(int count) {
    int current = windowSize.get();
    if (current + count > maxWindowSize) {
      throw new WindowFullException(
          "Window full: current="
              + current
              + " + requested="
              + count
              + " exceeds max="
              + maxWindowSize
              + ". Apply backpressure (pause partition).");
    }
  }
}
