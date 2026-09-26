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
 * <p>Every record flows through: REGISTERED → IN_PROGRESS → DONE (or FAILED). The tracker ensures
 * that {@link #getCommittableOffset} never returns an offset beyond contiguous completed records,
 * preventing silent data loss on crash/rebalance.
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
   * @throws IllegalStateException if the partition is not initialized or offset is duplicate
   */
  void register(TopicPartition tp, long offset);

  /**
   * Registers a batch of offsets.
   *
   * @throws IllegalArgumentException if offsets is empty or null
   * @throws IllegalStateException if any offset in the range overlaps with existing entries
   */
  void registerBatch(TopicPartition tp, long[] offsets);

  // ── Worker calls ──────────────────────────────────────────────

  /**
   * Marks an offset as IN_PROGRESS (worker has picked it up).
   *
   * @throws IllegalStateException if the offset is not in REGISTERED state
   */
  void markInProgress(TopicPartition tp, long offset);

  /**
   * Marks a batch of offsets as IN_PROGRESS. Used by batch processing mode.
   *
   * @throws IllegalStateException if any offset is not in REGISTERED state
   */
  void markBatchInProgress(TopicPartition tp, long[] offsets);

  /**
   * Marks an offset as DONE (processing succeeded, DLQ succeeded, or skipped). This advances the
   * committable offset if the acked record is contiguous with previously completed records.
   *
   * @throws IllegalStateException if the offset is not in IN_PROGRESS state
   */
  void ack(TopicPartition tp, long offset);

  /**
   * Marks a batch of offsets as DONE. Used by batch processing mode.
   *
   * @throws IllegalStateException if any offset is not in IN_PROGRESS state
   */
  void ackBatch(TopicPartition tp, long[] offsets);

  /**
   * Marks an offset as FAILED and puts the partition into failed state. No further records will be
   * accepted for this partition until it is cleared or the failure is resolved via {@link
   * #resolveFailure}.
   *
   * @throws IllegalStateException if the offset is not in IN_PROGRESS state
   */
  void fail(TopicPartition tp, long offset);

  /**
   * Marks a batch of offsets as FAILED and puts the partition into failed state. Used by batch
   * processing mode when all offsets in a batch fail together.
   *
   * @throws IllegalStateException if any offset is not in IN_PROGRESS state
   */
  void failBatch(TopicPartition tp, long[] offsets);

  /**
   * Resolves a previously failed offset by treating it as DONE (e.g., after sending to DLQ or
   * deciding to skip). Clears the partition's failed state so processing can resume.
   *
   * @throws IllegalStateException if the offset is not in FAILED state
   */
  void resolveFailure(TopicPartition tp, long offset);

  /**
   * Resolves a batch of previously failed offsets by treating them as DONE. Clears the partition's
   * failed state so processing can resume.
   *
   * @throws IllegalStateException if any offset is not in FAILED state
   */
  void resolveBatchFailure(TopicPartition tp, long[] offsets);

  // ── Committer calls ───────────────────────────────────────────

  /**
   * Returns the offset safe to commit for this partition, or empty if no progress has been made
   * since initialization.
   *
   * <p>The returned value follows Kafka semantics: committing N means "all records before N are
   * processed; start from N on next poll".
   */
  OptionalLong getCommittableOffset(TopicPartition tp);

  /**
   * Returns committable offsets for all tracked partitions that have made progress. Partitions with
   * no progress are omitted from the result.
   */
  Map<TopicPartition, Long> getAllCommittableOffsets();

  // ── Rebalance / Lifecycle ─────────────────────────────────────

  /**
   * Initializes tracking for a newly assigned partition. Called from {@code onPartitionsAssigned}.
   *
   * @param startOffset the offset from which this consumer will start polling
   * @throws IllegalStateException if the partition is already initialized
   */
  void initPartition(TopicPartition tp, long startOffset);

  /**
   * Waits for in-progress records on this partition to complete, up to the given timeout. Returns a
   * summary of how many records completed vs. were abandoned. Called from {@code
   * onPartitionsRevoked} before committing final offsets.
   */
  PartitionDrainResult drainPartition(TopicPartition tp, Duration timeout);

  /**
   * Removes all tracking state for this partition. Called after drain + commit during rebalance, or
   * during shutdown cleanup.
   */
  void clearPartition(TopicPartition tp);

  // ── Monitoring ────────────────────────────────────────────────

  /** Number of records in REGISTERED state (dispatched to queue but not yet picked up). */
  int pendingCount(TopicPartition tp);

  /** Number of records in IN_PROGRESS state (currently being processed by workers). */
  int inProgressCount(TopicPartition tp);

  /**
   * Distance between the highest registered offset and the committable offset. Useful for
   * monitoring how far behind processing is from polling.
   */
  long lag(TopicPartition tp);
}
