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
package io.github.openlogiclab.kafkapipeline.consumer;

import java.time.Duration;
import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

/**
 * Told when partitions leave this consumer, so components holding work for them can hurry it up and
 * then forget them. Both calls run on the poll thread and are not made during shutdown.
 */
public interface PartitionDrainListener {

  /**
   * The partitions are leaving. Called before the pipeline waits for their in-flight records.
   *
   * @param partitions the partitions
   * @param budget time the drain may take: the drain timeout for a revoke, zero for lost partitions
   *     (nothing can be committed for them any more)
   */
  void draining(Collection<TopicPartition> partitions, Duration budget);

  /**
   * The partitions are gone: drained and committed (revoke), or dropped (lost).
   *
   * @param partitions the partitions
   */
  default void drained(Collection<TopicPartition> partitions) {}
}
