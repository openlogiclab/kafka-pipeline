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
 * Identifies which assignment of a partition a record belongs to, so work from before a revoke
 * cannot touch the window created when the partition comes back.
 */
public sealed interface PartitionGeneration permits ProcessingProgress, PartitionLifecycle {

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
  long generation(TopicPartition tp);
}
