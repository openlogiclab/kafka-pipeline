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

import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/** What the committer needs: offsets safe to commit, and confirmation once Kafka has them. */
public sealed interface CommitSource permits OffsetTracker {

  /**
   * Returns commit points only for partitions with uncommitted progress, for periodic commits. Each
   * carries commit metadata recording offsets beyond the committed one that already finished. A
   * partition is included when its committable offset advanced or that set of finished offsets
   * changed since the last {@link #markCommitted}.
   *
   * @return map of partition to commit point
   */
  Map<TopicPartition, CommitPoint> getUncommittedCommitPoints();

  /**
   * Returns commit points for all tracked partitions that made progress, for sync commits on drain
   * and shutdown. Also includes a partition that has not advanced but has finished offsets beyond
   * its start.
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
}
