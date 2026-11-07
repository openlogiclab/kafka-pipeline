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

import org.apache.kafka.common.TopicPartition;

/**
 * The record lifecycle as workers see it: register a record before dispatch, mark it in progress
 * when a worker starts it, and ack it when it is done (or failed and skipped).
 */
public sealed interface ProcessingProgress extends PartitionGeneration permits OffsetTracker {

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
   * PartitionLifecycle #initPartition(TopicPartition, long, String)}). An already-finished offset
   * is recorded as DONE right away and must not be dispatched.
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

  /**
   * Records that an unrecoverable failure occurred on this partition.
   *
   * @param tp the topic partition
   */
  void markFailed(TopicPartition tp);

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
  void markInProgress(TopicPartition tp, long offset, long generation);

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
  int markBatchInProgress(TopicPartition tp, long[] offsets, long generation);

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
  void ack(TopicPartition tp, long offset, long generation);

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
  int ackBatch(TopicPartition tp, long[] offsets, long generation);
}
