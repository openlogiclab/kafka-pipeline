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

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * User-provided callback for processing a single Kafka record.
 *
 * <p>This library guarantees <b>at-least-once</b> delivery: after an ungraceful crash or rebalance,
 * some records may be delivered again. Implementations should be <b>idempotent</b> — processing the
 * same record twice must produce the same outcome as processing it once.
 *
 * <p>Common idempotency strategies:
 *
 * <ul>
 *   <li>DB upsert (INSERT ... ON CONFLICT DO UPDATE)
 *   <li>Deduplication table keyed by topic:partition:offset
 *   <li>Conditional writes with version/timestamp guards
 *   <li>Idempotency keys on external API calls
 * </ul>
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface RecordHandler<K, V> {

  /**
   * Process a single record. Called by a worker thread.
   *
   * <p>If this method returns normally, the record is considered successfully processed and the
   * offset will be eligible for commit.
   *
   * <p>If this method throws, the error strategy configured on the pipeline determines what happens
   * next (retry, send to DLQ, fail partition, or skip).
   *
   * @param record the Kafka record to process
   * @throws Exception if processing fails
   */
  void handle(ConsumerRecord<K, V> record) throws Exception;
}
