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
package io.github.openlogiclab.kafkapipeline.sink;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

/**
 * Identity of the input record whose outputs are being written.
 *
 * <p>The same input record always has the same {@link #recordId()}, however often it is processed,
 * so sinks can use it to make writes idempotent: an upsert key, a deterministic object key, an
 * idempotency header, or a dedup key downstream.
 *
 * @param partition topic-partition of the input record
 * @param offset offset of the input record
 * @param headers headers of the input record (read-only by convention)
 */
public record SinkContext(TopicPartition partition, long offset, Headers headers) {

  /**
   * Validates the components.
   *
   * @param partition topic-partition of the input record
   * @param offset offset of the input record
   * @param headers headers of the input record
   */
  public SinkContext {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(headers, "headers");
  }

  /**
   * Returns a stable identifier of the input record.
   *
   * @return {@code "<topic>-<partition>-<offset>"}
   */
  public String recordId() {
    return recordId(partition, offset);
  }

  /**
   * Returns the stable identifier of a record, as {@link #recordId()} does.
   *
   * @param partition the record's topic-partition
   * @param offset the record's offset
   * @return {@code "<topic>-<partition>-<offset>"}
   */
  public static String recordId(TopicPartition partition, long offset) {
    return partition.topic() + "-" + partition.partition() + "-" + offset;
  }
}
