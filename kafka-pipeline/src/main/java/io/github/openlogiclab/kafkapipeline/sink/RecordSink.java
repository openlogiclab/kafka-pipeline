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
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.TopicPartition;

/**
 * Writes a pipeline's output downstream: Kafka, a database, object storage, HTTP, or anything else.
 *
 * <p>A record is acked (and becomes committable) only when the stage returned by {@link #write}
 * completes. Complete it only once the output is durable downstream. Completing it exceptionally
 * retries the write, never the processor; how depends on the failure (see {@link #classify}): a
 * transient failure is retried until the destination recovers, a fatal one stops the pipeline, and
 * a record-specific one is retried up to the error strategy's {@code maxRetries} before the record
 * goes to the DLQ or is dropped. Records count as in flight for global backpressure until their
 * write completes.
 *
 * <p>Delivery is at-least-once. A retried write sends the record's output again, even if it already
 * landed, and a record is written again after a crash or rebalance. Use {@link
 * SinkContext#recordId()} to make writes idempotent downstream.
 *
 * <p>For several outputs per record, use {@link #forEach(RecordSink)}: the processor returns a
 * {@code List} and each element is written with this sink.
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
   * Writes the output of one input record.
   *
   * @param ctx identity of the input record, for idempotent writes
   * @param output the output; never null
   * @return a stage that completes when the output is durable, or completes exceptionally if it
   *     cannot be written
   */
  CompletionStage<Void> write(SinkContext ctx, O output);

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
   * while the pipeline waits for them. From this call on, a buffering sink must write their output
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
    Objects.requireNonNull(writer, "writer");
    return (ctx, output) -> {
      try {
        writer.write(ctx, output);
        return CompletableFuture.completedFuture(null);
      } catch (Exception e) {
        // The interrupt belongs to the pipeline (it decides whether it means shutdown).
        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(e);
      }
    };
  }

  /**
   * Writes every element of a list with {@code sink}, for processors that return several outputs
   * per record ({@code Optional<List<E>>}). The record is acked once every element is durable; an
   * empty list is acked without a write, and a list with a null element fails before anything is
   * written. Element {@code i} is written with {@code ctx.forOutput(i)}, so {@link
   * SinkContext#outputId()} tells the elements apart.
   *
   * <p>If any element fails, the write fails once all of them have completed, with the most severe
   * failure by {@code sink}'s {@link #classify} (the others are suppressed), and a retry writes the
   * whole list again: elements that already landed are written twice, so make writes idempotent
   * with {@link SinkContext#outputId()}.
   *
   * <p>Elements are handed to {@code sink} in list order, concurrently; whether they land in that
   * order is up to the sink (a Kafka producer keeps the order of records sent to one partition).
   * Everything else ({@link #flush}, {@link #onPartitionRevoked}, {@link #sensor}, {@link
   * #classify}, {@link #maxBufferedRecordsPerPartition}, {@link #close}) is {@code sink}'s.
   *
   * <pre>{@code
   * .processor(record -> Optional.of(splitIntoLines(record.value())))   // Optional<List<Line>>
   * .sink(RecordSink.forEach(lineSink))                                  // RecordSink<Line>
   * }</pre>
   *
   * @param sink writes one element
   * @param <E> element type
   * @return a sink of lists
   */
  static <E> RecordSink<List<E>> forEach(RecordSink<E> sink) {
    return new ForEachSink<>(sink);
  }

  /**
   * Blocking write code, for {@link RecordSink#blocking(BlockingWriter)}.
   *
   * @param <O> output type
   */
  @FunctionalInterface
  interface BlockingWriter<O> {
    /**
     * Writes the output; returns once it is durable.
     *
     * @param ctx identity of the input record
     * @param output the output
     * @throws Exception if the write fails
     */
    void write(SinkContext ctx, O output) throws Exception;
  }
}
