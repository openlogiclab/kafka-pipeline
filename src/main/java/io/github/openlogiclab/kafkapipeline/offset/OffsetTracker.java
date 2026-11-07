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
import org.apache.kafka.common.TopicPartition;

/**
 * Central source of truth for record lifecycle and offset commit safety.
 *
 * <p>Every record flows through: REGISTERED → IN_PROGRESS → DONE. The tracker ensures that {@link
 * #getCommittableOffset} never returns an offset beyond contiguous completed records, preventing
 * silent data loss on crash/rebalance.
 *
 * <p>Uses a sliding window approach that supports concurrent out-of-order processing per partition.
 *
 * <p>All methods are thread-safe. Offset values follow Kafka commit semantics: committing N means
 * "next poll starts from offset N".
 */
public sealed interface OffsetTracker permits UnorderedOffsetTracker {

  // ── Poller calls ──────────────────────────────────────────────

  /**
   * Registers a single offset as REGISTERED (pending dispatch to a worker). Must be called before
   * the record is dispatched.
   *
   * @param tp the topic partition
   * @param offset the offset to register
   * @throws IllegalStateException if the partition is not initialized or offset is duplicate
   */
  void register(TopicPartition tp, long offset);

  /**
   * Registers a batch of offsets.
   *
   * @param tp the topic partition
   * @param offsets the offsets to register
   * @throws IllegalArgumentException if offsets is empty or null
   * @throws IllegalStateException if any offset in the range overlaps with existing entries
   */
  void registerBatch(TopicPartition tp, long[] offsets);

  /**
   * Registers an offset, unless the previous owner of the partition already finished it (see {@link
   * #initPartition(TopicPartition, long, String)}). An already-finished offset is recorded as DONE
   * right away and must not be dispatched.
   *
   * @param tp the topic partition
   * @param offset the offset to register
   * @return true if the record must be processed, false if it was already done
   */
  boolean registerUnlessDone(TopicPartition tp, long offset);

  /**
   * Batch form of {@link #registerUnlessDone(TopicPartition, long)}.
   *
   * @param tp the topic partition
   * @param offsets the offsets to register
   * @return the offsets that must be processed, in order; empty if all were already done
   */
  long[] registerBatchUnlessDone(TopicPartition tp, long[] offsets);

  // ── Worker calls ──────────────────────────────────────────────

  /**
   * Marks an offset as IN_PROGRESS (worker has picked it up).
   *
   * @param tp the topic partition
   * @param offset the offset to mark
   * @throws IllegalStateException if the offset is not in REGISTERED state
   */
  void markInProgress(TopicPartition tp, long offset);

  /**
   * Marks a batch of offsets as IN_PROGRESS. Lenient: skips offsets not in REGISTERED state.
   *
   * @param tp the topic partition
   * @param offsets the offsets to mark
   * @return number of offsets actually transitioned
   */
  int markBatchInProgress(TopicPartition tp, long[] offsets);

  /**
   * Marks an offset as DONE (processing succeeded, sent to DLQ, or skipped). This advances the
   * committable offset if the acked record is contiguous with previously completed records.
   *
   * @param tp the topic partition
   * @param offset the offset to ack
   * @throws IllegalStateException if the offset is not in IN_PROGRESS state
   */
  void ack(TopicPartition tp, long offset);

  /**
   * Marks a batch of offsets as DONE. Lenient: skips offsets not in IN_PROGRESS state.
   *
   * @param tp the topic partition
   * @param offsets the offsets to ack
   * @return number of offsets actually acked
   */
  int ackBatch(TopicPartition tp, long[] offsets);

  // ── Committer calls ───────────────────────────────────────────

  /**
   * Returns the offset safe to commit for this partition, or empty if no progress has been made
   * since initialization.
   *
   * <p>The returned value follows Kafka semantics: committing N means "all records before N are
   * processed; start from N on next poll".
   *
   * @param tp the topic partition
   * @return the committable offset, or empty if no progress
   */
  OptionalLong getCommittableOffset(TopicPartition tp);

  /**
   * Returns committable offsets for all tracked partitions that have made progress. Partitions with
   * no progress are omitted from the result.
   *
   * @return map of partition to committable offset
   */
  Map<TopicPartition, Long> getAllCommittableOffsets();

  /**
   * Returns committable offsets only for partitions that have uncommitted progress (offset advanced
   * since last {@link #markCommitted} call). Used for periodic commits to avoid redundant commits.
   *
   * @return map of partition to uncommitted offset
   */
  Map<TopicPartition, Long> getUncommittedOffsets();

  /**
   * Marks an offset as confirmed committed to Kafka. Called from async commit callback. Should be
   * called for each partition after a successful commit.
   *
   * @param tp the topic partition
   * @param offset the committed offset
   */
  void markCommitted(TopicPartition tp, long offset);

  /**
   * Like {@link #getUncommittedOffsets()}, but each entry also carries commit metadata recording
   * offsets beyond the committed one that already finished. A partition is included when its
   * committable offset advanced or that set of finished offsets changed.
   *
   * @return map of partition to commit point
   */
  Map<TopicPartition, CommitPoint> getUncommittedCommitPoints();

  /**
   * Like {@link #getAllCommittableOffsets()}, with commit metadata. Also includes a partition that
   * has not advanced but has finished offsets beyond its start.
   *
   * @return map of partition to commit point
   */
  Map<TopicPartition, CommitPoint> getAllCommitPoints();

  /**
   * Marks a commit point as confirmed committed to Kafka.
   *
   * @param tp the topic partition
   * @param point the committed point
   */
  void markCommitted(TopicPartition tp, CommitPoint point);

  /**
   * Stops recording finished offsets in commit metadata, for every partition now and later. Called
   * when the broker rejects the metadata as too large ({@code offset.metadata.max.bytes}), so
   * commits fall back to plain offsets instead of failing every time.
   */
  void stopTrackingCompleted();

  // ── Rebalance / Lifecycle ─────────────────────────────────────

  /**
   * Initializes tracking for a newly assigned partition. Called from {@code onPartitionsAssigned}.
   *
   * @param tp the topic partition
   * @param startOffset the offset from which this consumer will start polling
   * @throws IllegalStateException if the partition is already initialized
   */
  void initPartition(TopicPartition tp, long startOffset);

  /**
   * Initializes tracking for a newly assigned partition, using the metadata committed with the
   * offset the consumer resumes from. If that metadata lists offsets the previous owner already
   * finished, those records are skipped instead of processed again. Metadata that is missing,
   * unreadable or relative to a different offset is ignored.
   *
   * @param tp the topic partition
   * @param startOffset the offset from which this consumer will start polling
   * @param committedMetadata the metadata committed with {@code startOffset}, or null
   * @throws IllegalStateException if the partition is already initialized
   */
  void initPartition(TopicPartition tp, long startOffset, String committedMetadata);

  /**
   * Waits for in-progress records on this partition to complete, up to the given timeout. Returns a
   * summary of how many records completed vs. were abandoned. Called from {@code
   * onPartitionsRevoked} before committing final offsets.
   *
   * @param tp the topic partition
   * @param timeout the maximum time to wait
   * @return result containing completion and abandonment counts
   */
  PartitionDrainResult drainPartition(TopicPartition tp, Duration timeout);

  /**
   * Removes all tracking state for this partition. Called after drain + commit during rebalance, or
   * during shutdown cleanup.
   *
   * @param tp the topic partition
   */
  void clearPartition(TopicPartition tp);

  // ── Monitoring ────────────────────────────────────────────────

  /**
   * Number of records in REGISTERED state (dispatched to queue but not yet picked up).
   *
   * @param tp the topic partition
   * @return pending record count
   */
  int pendingCount(TopicPartition tp);

  /**
   * Number of records in IN_PROGRESS state (currently being processed by workers).
   *
   * @param tp the topic partition
   * @return in-progress record count
   */
  int inProgressCount(TopicPartition tp);

  /**
   * Distance between the highest registered offset and the committable offset. Useful for
   * monitoring how far behind processing is from polling.
   *
   * @param tp the topic partition
   * @return the lag count
   */
  long lag(TopicPartition tp);

  /**
   * Returns true if any unrecoverable failure has occurred on this partition.
   *
   * @param tp the topic partition
   * @return true if failed
   */
  boolean isFailed(TopicPartition tp);

  /**
   * Records that an unrecoverable failure occurred on this partition.
   *
   * @param tp the topic partition
   */
  void markFailed(TopicPartition tp);

  /**
   * Number of unrecoverable failures that have occurred on this partition.
   *
   * @param tp the topic partition
   * @return failure count
   */
  int failureCount(TopicPartition tp);

  /**
   * Number of entries currently held in the partition's window, including completed offsets that
   * are waiting behind an unfinished one.
   *
   * @param tp the topic partition
   * @return window size, or 0 if the partition is not tracked
   */
  int windowSize(TopicPartition tp);

  /**
   * Maximum number of entries a partition window can hold before registration fails with {@link
   * WindowFullException}.
   *
   * @return the per-partition window capacity
   */
  int maxWindowSize();

  /** Generation value meaning "unknown": operations given it skip the generation check. */
  long NO_GENERATION = -1L;

  /**
   * Returns a token identifying the partition's current window. It changes every time the partition
   * is initialized, so a record fetched before a revoke can be told apart from the same offset
   * fetched again after the partition is re-assigned to this consumer.
   *
   * @param tp the topic partition
   * @return the current generation, or {@link #NO_GENERATION} if the partition is not tracked
   */
  default long generation(TopicPartition tp) {
    return NO_GENERATION;
  }

  /**
   * Like {@link #markInProgress(TopicPartition, long)}, but only if the partition's window is still
   * the given generation.
   *
   * @param tp the topic partition
   * @param offset the offset
   * @param generation the generation the record was registered under
   * @throws IllegalStateException if the partition is not tracked, its window is a different
   *     generation, or the offset is not REGISTERED
   */
  default void markInProgress(TopicPartition tp, long offset, long generation) {
    markInProgress(tp, offset);
  }

  /**
   * Like {@link #markBatchInProgress(TopicPartition, long[])}, but only if the partition's window
   * is still the given generation.
   *
   * @param tp the topic partition
   * @param offsets the offsets
   * @param generation the generation the batch was registered under
   * @return number of offsets transitioned
   * @throws IllegalStateException if the partition is not tracked or its window is a different
   *     generation
   */
  default int markBatchInProgress(TopicPartition tp, long[] offsets, long generation) {
    return markBatchInProgress(tp, offsets);
  }

  /**
   * Like {@link #ack(TopicPartition, long)}, but only if the partition's window is still the given
   * generation. A late ack from before a revoke must not complete the same offset in the window
   * created when the partition came back.
   *
   * @param tp the topic partition
   * @param offset the offset
   * @param generation the generation the record was registered under
   * @throws IllegalStateException if the partition is not tracked, its window is a different
   *     generation, or the offset is not IN_PROGRESS
   */
  default void ack(TopicPartition tp, long offset, long generation) {
    ack(tp, offset);
  }

  /**
   * Like {@link #ackBatch(TopicPartition, long[])}, but only if the partition's window is still the
   * given generation.
   *
   * @param tp the topic partition
   * @param offsets the offsets
   * @param generation the generation the batch was registered under
   * @return number of offsets transitioned
   * @throws IllegalStateException if the partition is not tracked or its window is a different
   *     generation
   */
  default int ackBatch(TopicPartition tp, long[] offsets, long generation) {
    return ackBatch(tp, offsets);
  }
}
