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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.TopicPartition;

/**
 * Writes a pipeline's outputs downstream: Kafka, a database, object storage, HTTP, or anything
 * else.
 *
 * <p>A record is acked (and becomes committable) only when the stage returned by {@link #write}
 * completes. Complete it only once the outputs are durable downstream. Completing it exceptionally
 * retries the write, never the processor; how depends on the failure (see {@link #classify}): a
 * transient failure is retried until the destination recovers, a fatal one stops the pipeline, and
 * a record-specific one is retried up to the error strategy's {@code maxRetries} before the record
 * goes to the DLQ or is dropped. Records count as in flight for global backpressure until their
 * write completes.
 *
 * <p>Delivery is at-least-once. A retried write sends all of a record's outputs again, including
 * any that already landed, and a record is written again after a crash or rebalance. Use {@link
 * SinkContext#recordId()} to make writes idempotent downstream.
 *
 * <p>{@link #write} is called concurrently by many workers on one instance, so implementations must
 * be thread-safe. It must not block for long: use an asynchronous client, or wrap blocking code
 * with {@link #blocking(BlockingWriter)}.
 *
 * <p>Only {@link #write} is abstract, so simple sinks can be lambdas or method references. Sinks
 * that buffer, flush, react to revoked partitions or report downstream pressure override the
 * defaults and are classes.
 *
 * @param <O> output type, as returned by the {@link RecordProcessor}
 */
@FunctionalInterface
public interface RecordSink<O> extends AutoCloseable {

  /**
   * Writes the outputs of one input record.
   *
   * @param ctx identity of the input record, for idempotent writes
   * @param outputs the outputs; never empty
   * @return a stage that completes when the outputs are durable, or completes exceptionally if they
   *     cannot be written
   */
  CompletionStage<Void> write(SinkContext ctx, List<O> outputs);

  /**
   * Pushes out data this sink buffers for the partition. Called at shutdown for every assigned
   * partition; the pipeline waits for the returned stages within its shutdown budget.
   *
   * @param partition the input partition
   * @return a stage that completes when the buffered data is written
   */
  default CompletionStage<Void> flush(TopicPartition partition) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * The input partition is leaving this consumer. A buffering sink must write what it holds for it
   * within {@code budget} (records written in time are committed) or drop it (dropped records are
   * redelivered to the next owner). A zero budget means the partition was lost and nothing can be
   * committed for it anymore.
   *
   * <p>Records of the partition that were still being processed may be written after this call,
   * while the pipeline waits for them. From this call on, a buffering sink must write such outputs
   * for the partition at once instead of buffering them; otherwise the pipeline waits until the
   * drain timeout and the records are redelivered.
   *
   * @param partition the input partition
   * @param budget time available, at most the pipeline's drain timeout
   */
  default void onPartitionRevoked(TopicPartition partition, Duration budget) {}

  /**
   * Downstream pressure the pipeline cannot see on its own, such as unsent producer records or a
   * saturated connection pool. It is added to the pipeline's global backpressure, so it throttles
   * all partitions.
   *
   * @return the sensor, or empty
   */
  default Optional<BackpressureSensor> sensor() {
    return Optional.empty();
  }

  /**
   * Classifies a write failure, which decides what the pipeline does next (see {@link
   * WriteFailure}): {@code TRANSIENT} is retried until the destination recovers (an outage becomes
   * backpressure, not dropped records), {@code FATAL} stops the pipeline with the record unacked,
   * and {@code RECORD} is retried up to the error strategy's {@code maxRetries} before the record
   * goes to the DLQ or is dropped. JVM errors such as {@code OutOfMemoryError} always stop the
   * pipeline.
   *
   * <p>The default, {@link WriteFailure#of(Throwable)}, understands Kafka client errors. Override
   * it for other clients, for example to treat {@code java.sql.SQLTransientException} as {@code
   * TRANSIENT} and {@code java.sql.SQLInvalidAuthorizationSpecException} as {@code FATAL}.
   *
   * @param error the write failure
   * @return its classification
   */
  default WriteFailure classify(Throwable error) {
    return WriteFailure.of(error);
  }

  /**
   * Largest number of input records this sink may hold unwritten for one partition, for sinks that
   * buffer. Must be below 80% of the pipeline's {@code maxWindowSize}, or the partition would pause
   * before the buffer can fill.
   *
   * @return the bound, or empty if the sink does not buffer
   */
  default OptionalInt maxBufferedRecordsPerPartition() {
    return OptionalInt.empty();
  }

  /**
   * Releases resources the sink owns. Called once at shutdown, after the final commit.
   *
   * @throws Exception if closing fails; it is logged
   */
  @Override
  default void close() throws Exception {}

  /**
   * Wraps blocking write code as a sink. The writer runs on the worker that processed the record,
   * and the stage completes when it returns, so the worker waits for the write. That is cheap with
   * virtual threads; with platform threads every write in progress holds a worker.
   *
   * @param writer the blocking writer
   * @param <O> output type
   * @return a sink running the writer
   */
  static <O> RecordSink<O> blocking(BlockingWriter<O> writer) {
    return (ctx, outputs) -> {
      try {
        writer.write(ctx, outputs);
        return CompletableFuture.completedFuture(null);
      } catch (Exception e) {
        // The interrupt belongs to the pipeline (it decides whether it means shutdown).
        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(e);
      }
    };
  }

  /**
   * Blocking write code, for {@link RecordSink#blocking(BlockingWriter)}.
   *
   * @param <O> output type
   */
  @FunctionalInterface
  interface BlockingWriter<O> {
    /**
     * Writes the outputs; returns once they are durable.
     *
     * @param ctx identity of the input record
     * @param outputs the outputs
     * @throws Exception if the write fails
     */
    void write(SinkContext ctx, List<O> outputs) throws Exception;
  }
}
