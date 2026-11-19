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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Partition assignment as the rebalance listener drives it: start, drain and stop tracking. */
public sealed interface PartitionLifecycle extends PartitionGeneration permits OffsetTracker {

  /**
   * Initializes tracking for a newly assigned partition whose group offset is already committed at
   * {@code startOffset}, without metadata. Shorthand for {@code initPartition(tp, startOffset, new
   * OffsetAndMetadata(startOffset))}.
   *
   * @param tp the topic partition
   * @param startOffset the offset from which this consumer will start polling
   * @throws IllegalStateException if the partition is already initialized
   */
  void initPartition(TopicPartition tp, long startOffset);

  /**
   * Initializes tracking for a newly assigned partition, given what the group has committed for it.
   * Called from {@code onPartitionsAssigned}.
   *
   * <p>If the committed offset is {@code startOffset} and its metadata lists offsets the previous
   * owner already finished, those records are skipped instead of processed again (metadata that is
   * unreadable is ignored). If nothing is committed, or the committed offset is not {@code
   * startOffset} (the consumer reset or sought away from it), the first commit writes {@code
   * startOffset} even before any record finishes, so the next owner resumes here instead of
   * applying {@code auto.offset.reset} and skipping the records this consumer did not finish.
   *
   * @param tp the topic partition
   * @param startOffset the offset from which this consumer will start polling
   * @param committed the group's committed offset for the partition, or null if there is none
   * @throws IllegalStateException if the partition is already initialized
   */
  void initPartition(TopicPartition tp, long startOffset, OffsetAndMetadata committed);

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
