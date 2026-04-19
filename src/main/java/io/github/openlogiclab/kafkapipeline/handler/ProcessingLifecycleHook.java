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
 * Optional lifecycle hook for cross-cutting concerns around record processing.
 *
 * <p>This is the extension point for users who want to plug in custom behavior without modifying
 * their {@link RecordHandler}. Typical use cases:
 *
 * <ul>
 *   <li><b>Deduplication</b> — check a dedup store in {@link #beforeProcess} and skip duplicates by
 *       returning {@code false}
 *   <li><b>Metrics</b> — measure processing latency between before/after
 *   <li><b>Audit logging</b> — record what was processed and when
 *   <li><b>Distributed tracing</b> — extract/inject trace context from headers
 * </ul>
 *
 * <p>All methods have safe default implementations (no-op / proceed). Override only what you need.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public interface ProcessingLifecycleHook<K, V> {

  /**
   * Called before the {@link RecordHandler} processes the record.
   *
   * <p>Return {@code false} to skip processing entirely (the record will be acked as DONE without
   * calling the handler). This is the idempotency / deduplication hook.
   *
   * @param record the record about to be processed
   * @param context metadata (partition, offset, attempt number)
   * @return {@code true} to proceed with processing, {@code false} to skip
   */
  default boolean beforeProcess(ConsumerRecord<K, V> record, ProcessingContext context) {
    return true;
  }

  /**
   * Called after the {@link RecordHandler} completes successfully.
   *
   * <p>Use this to record the successful processing in a dedup store, emit metrics, or close
   * tracing spans.
   *
   * @param record the record that was processed
   * @param context metadata (partition, offset, attempt number)
   */
  default void afterProcess(ConsumerRecord<K, V> record, ProcessingContext context) {}

  /**
   * Called when the {@link RecordHandler} throws an exception. This is purely observational — error
   * handling strategy (retry, DLQ, skip, fail) is configured separately on the pipeline.
   *
   * @param record the record that failed
   * @param context metadata (partition, offset, attempt number)
   * @param error the exception thrown by the handler
   */
  default void onError(ConsumerRecord<K, V> record, ProcessingContext context, Exception error) {}

  /** A no-op hook that proceeds with all records and does nothing on completion/error. */
  static <K, V> ProcessingLifecycleHook<K, V> noOp() {
    return new ProcessingLifecycleHook<>() {};
  }
}
