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
 * Final preservation handler — the last chance to save a record before it is permanently dropped.
 *
 * <p>Called when all recovery options are exhausted (retries failed AND DLQ failed or not
 * configured). After this handler returns, the record is skipped, the <b>offset is committed</b>,
 * and the data is <b>permanently lost</b> from the pipeline's perspective.
 *
 * <h2>Trade-off</h2>
 *
 * <p>This library prioritizes <b>throughput over blocking on unprocessable data</b>. Records that
 * cannot be processed (after retries and DLQ) are skipped, and the offset advances immediately so
 * the pipeline keeps moving. If this handler doesn't preserve the data, it cannot be recovered.
 * Choose your preservation strategy based on data criticality:
 *
 * <ul>
 *   <li>Log the full record (key, value, headers, metadata) for later analysis
 *   <li>Write to a local file or persistent volume (PVC)
 *   <li>Upload to object storage (S3, GCS, Azure Blob)
 *   <li>Send to an alerting system (PagerDuty, Slack, email)
 *   <li>Increment custom metrics for monitoring
 * </ul>
 *
 * <p>The default implementation logs at ERROR level (metadata only, not raw data). For production
 * systems processing critical data, implement a more robust preservation strategy.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface FinalFailureHandler<K, V> {

  /**
   * Preserves or records information about an unrecoverable failure.
   *
   * <p>This is the final opportunity to capture the record before it is dropped. The implementation
   * should be resilient — if this handler throws, the exception is logged but the record is still
   * skipped.
   *
   * @param record the failed record (contains key, value, topic, partition, offset, headers)
   * @param error the final exception (from DLQ failure or last processing attempt)
   */
  void handle(ConsumerRecord<K, V> record, Exception error);
}
