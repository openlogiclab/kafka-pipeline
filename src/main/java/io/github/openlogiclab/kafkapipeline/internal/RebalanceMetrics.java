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
package io.github.openlogiclab.kafkapipeline.internal;

import org.apache.kafka.common.TopicPartition;

/**
 * Metrics the rebalance listener records.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public interface RebalanceMetrics {

  /**
   * Records that a partition was assigned to this consumer.
   *
   * @param tp the assigned partition
   */
  void partitionAssigned(TopicPartition tp);

  /**
   * Records that a partition was revoked from this consumer.
   *
   * @param tp the revoked partition
   */
  void partitionRevoked(TopicPartition tp);

  /** Records a consumer group rebalance. */
  void recordRebalance();

  /** Records a partition drain that did not finish within the drain timeout. */
  void recordDrainTimeout();

  /**
   * Records records abandoned during a rebalance drain.
   *
   * @param count number of records abandoned
   */
  void recordAbandoned(long count);
}
