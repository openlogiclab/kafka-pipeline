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
}
