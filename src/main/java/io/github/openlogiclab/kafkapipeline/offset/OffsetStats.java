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

import java.util.OptionalLong;
import org.apache.kafka.common.TopicPartition;

/** Read-only view of per-partition tracking state, for metrics and flow control. */
public sealed interface OffsetStats permits OffsetTracker {

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
   * How long the partition's committable offset has not moved while it has records pending or in
   * progress.
   *
   * @param tp the topic partition
   * @return milliseconds since the committable offset last advanced, or 0 if nothing is waiting or
   *     the partition is not tracked
   */
  long commitStallMillis(TopicPartition tp);

  /**
   * Maximum number of entries a partition window can hold before registration fails with {@link
   * WindowFullException}.
   *
   * @return the per-partition window capacity
   */
  int maxWindowSize();
}
