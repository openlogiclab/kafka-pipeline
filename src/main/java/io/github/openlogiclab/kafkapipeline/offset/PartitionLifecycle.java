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
import org.apache.kafka.common.TopicPartition;

/** Partition assignment as the rebalance listener drives it: start, drain and stop tracking. */
public sealed interface PartitionLifecycle extends PartitionGeneration permits OffsetTracker {

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
}
