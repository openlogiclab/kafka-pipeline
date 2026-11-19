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
 * Identity of the output being written: the input record it comes from, and its position among that
 * record's outputs.
 *
 * <p>The same input record always has the same {@link #recordId()}, however often it is processed,
 * so sinks can use it to make writes idempotent: an upsert key, a deterministic object key, an
 * idempotency header, or a dedup key downstream. When a record has several outputs ({@link
 * RecordSink#forEach(RecordSink)}), they share the record id and differ in {@link #outputIndex()};
 * use {@link #outputId()} as the key then.
 *
 * @param partition topic-partition of the input record
 * @param offset offset of the input record
 * @param headers headers of the input record (read-only by convention)
 * @param outputIndex position of this output among the input record's outputs; 0 when there is one
 */
public record SinkContext(TopicPartition partition, long offset, Headers headers, int outputIndex) {

  /**
   * Validates the components.
   *
   * @param partition topic-partition of the input record
   * @param offset offset of the input record
   * @param headers headers of the input record
   * @param outputIndex position of this output among the input record's outputs, not negative
   */
  public SinkContext {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(headers, "headers");
    if (outputIndex < 0) throw new IllegalArgumentException("outputIndex < 0: " + outputIndex);
  }

  /**
   * The context of a record's only output (index 0).
   *
   * @param partition topic-partition of the input record
   * @param offset offset of the input record
   * @param headers headers of the input record
   */
  public SinkContext(TopicPartition partition, long offset, Headers headers) {
    this(partition, offset, headers, 0);
  }

  /**
   * The context of another output of the same input record.
   *
   * @param index position of that output among the record's outputs
   * @return the context
   */
  public SinkContext forOutput(int index) {
    return new SinkContext(partition, offset, headers, index);
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
   * Returns a stable identifier of this output: the record id and the output's index.
   *
   * @return {@code "<topic>-<partition>-<offset>-<outputIndex>"}
   */
  public String outputId() {
    return recordId() + "-" + outputIndex;
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
