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
package io.github.openlogiclab.kafkapipeline.error;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Sends a failed record to a dead letter queue (or any secondary destination).
 *
 * <p>If this method returns normally, the record is considered handled and will be acked (offset
 * advances). If it throws, the {@link Fallback} strategy decides what happens next.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface DLQHandler<K, V> {

  /**
   * @param record the record that failed processing
   * @param lastError the last exception thrown by the handler (after all retries)
   * @throws Exception if sending to DLQ fails
   */
  void send(ConsumerRecord<K, V> record, Exception lastError) throws Exception;
}
