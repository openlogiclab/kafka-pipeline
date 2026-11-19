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

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Processes one input record and returns the outputs for the pipeline's {@link RecordSink}.
 *
 * <p>This is the business logic of a pipeline that writes somewhere. It runs on the worker pool,
 * concurrently with other records (including other records of the same partition, unless an
 * ordering mode says otherwise), and may block on I/O such as database lookups or HTTP calls. It
 * returns 0..n outputs; an empty list means there is nothing to write, and the record is acked.
 *
 * <p>The record counts as processed only once the sink has written every output durably, so a
 * processor never needs to wait for the write itself.
 *
 * <p>A processor may run more than once for the same record: after a failed attempt (retries),
 * after a crash, or after a rebalance. External side effects inside it should therefore be
 * idempotent; the sink write is what the library tracks.
 *
 * <p>One instance is shared by all workers, so implementations must be thread-safe. Because this
 * interface has a single abstract method, a lambda, a method reference or a class all work:
 *
 * <pre>{@code
 * .processor(record -> List.of(enrich(record.value())))
 * .processor(orderService::enrich)
 * .processor(new OrderEnrichmentProcessor(customers, pricing))
 * }</pre>
 *
 * @param <K> input record key type
 * @param <V> input record value type
 * @param <O> output type, written by the sink
 */
@FunctionalInterface
public interface RecordProcessor<K, V, O> extends AutoCloseable {

  /**
   * Processes one record.
   *
   * @param record the input record
   * @return outputs for the sink, possibly empty; never null
   * @throws Exception to fail this attempt; the record is retried, then sent to the DLQ or dropped
   */
  List<O> process(ConsumerRecord<K, V> record) throws Exception;

  /**
   * Releases resources the processor owns. Called once at shutdown, after the last record has
   * finished and before the sinks are closed.
   *
   * @throws Exception if closing fails; it is logged
   */
  @Override
  default void close() throws Exception {}
}
