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
package io.github.openlogiclab.kafkapipeline.handler;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * User-provided callback for processing a batch of Kafka records from a single partition.
 *
 * <p>Batch mode is optimized for high-throughput scenarios where per-record overhead matters. The
 * entire batch from a single partition is delivered atomically — all records succeed or fail
 * together. Offset tracking uses a single lock acquisition per batch instead of per record.
 *
 * <p>This library guarantees <b>at-least-once</b> delivery: after an ungraceful crash or rebalance,
 * some records may be delivered again. Implementations should be <b>idempotent</b>.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface BatchRecordHandler<K, V> {

  /**
   * Process a batch of records from a single partition. Called by a worker thread.
   *
   * <p>If this method returns normally, all records in the batch are considered successfully
   * processed and their offsets will be eligible for commit.
   *
   * <p>If this method throws, the error strategy configured on the pipeline determines what happens
   * next (retry the entire batch, send to DLQ, fail partition, or skip).
   *
   * @param partition the source partition for all records in this batch
   * @param records the batch of records to process (never empty, all from the same partition)
   * @throws Exception if processing fails
   */
  void handleBatch(TopicPartition partition, List<ConsumerRecord<K, V>> records) throws Exception;
}
