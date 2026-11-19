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
package io.github.openlogiclab.kafkapipeline;

/**
 * Names of the headers this library adds to records it writes to Kafka: dead letters ({@code
 * error.KafkaDLQHandler}) and outputs of the Kafka sink ({@code sink.kafka.KafkaRecordSink}).
 */
public final class KafkaHeaders {

  private KafkaHeaders() {}

  /**
   * On every output and DLQ record: the input record's id, {@code topic-partition-offset} (see
   * {@link io.github.openlogiclab.kafkapipeline.sink.SinkContext#recordId()}). A redelivered input
   * produces the same id. Outputs of one input share it, so consumers drop duplicates by this
   * header together with {@link #OUTPUT_INDEX}.
   */
  public static final String SOURCE = "kp-source";

  /**
   * On every output of the Kafka sink: the output's position among its input record's outputs
   * ({@link io.github.openlogiclab.kafkapipeline.sink.SinkContext#outputIndex()}), {@code 0} when
   * there is one. {@link #SOURCE} and this header together identify an output.
   */
  public static final String OUTPUT_INDEX = "kp-output-index";

  /** On DLQ records: the input record's topic. */
  public static final String DLQ_TOPIC = "kp-dlq-topic";

  /** On DLQ records: the input record's partition. */
  public static final String DLQ_PARTITION = "kp-dlq-partition";

  /** On DLQ records: the input record's offset. */
  public static final String DLQ_OFFSET = "kp-dlq-offset";

  /** On DLQ records: the class of the last error. */
  public static final String DLQ_ERROR_CLASS = "kp-dlq-error-class";

  /** On DLQ records: the message of the last error, cut to 1,000 characters. */
  public static final String DLQ_ERROR_MESSAGE = "kp-dlq-error-message";
}
