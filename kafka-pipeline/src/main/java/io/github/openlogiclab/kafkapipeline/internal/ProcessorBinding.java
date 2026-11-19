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
package io.github.openlogiclab.kafkapipeline.internal;

import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.sink.RecordProcessor;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A processor and the sink its output goes to, with matching output types. A plain {@link
 * RecordHandler} is bound as a processor without output and without a sink, so both shapes of
 * pipeline share one processing path.
 *
 * @param <K> input key type
 * @param <V> input value type
 * @param <O> output type
 */
public final class ProcessorBinding<K, V, O> {

  private final RecordProcessor<K, V, ? extends O> processor;
  private final RecordSink<O> sink; // null for a handler: it writes nothing the library tracks
  private final AutoCloseable owner; // what to close for the processor step

  private ProcessorBinding(
      RecordProcessor<K, V, ? extends O> processor, RecordSink<O> sink, AutoCloseable owner) {
    this.processor = processor;
    this.sink = sink;
    this.owner = owner;
  }

  /**
   * Binds a processor to its sink.
   *
   * @param processor the processor
   * @param sink the sink
   * @param <K> input key type
   * @param <V> input value type
   * @param <O> output type
   * @return the binding
   */
  public static <K, V, O> ProcessorBinding<K, V, O> of(
      RecordProcessor<K, V, ? extends O> processor, RecordSink<O> sink) {
    Objects.requireNonNull(processor, "processor");
    Objects.requireNonNull(sink, "sink");
    return new ProcessorBinding<>(processor, sink, processor);
  }

  /**
   * Binds a handler as a processor without output.
   *
   * @param handler the handler
   * @param <K> input key type
   * @param <V> input value type
   * @return the binding
   */
  public static <K, V> ProcessorBinding<K, V, Void> forHandler(RecordHandler<K, V> handler) {
    Objects.requireNonNull(handler, "handler");
    RecordProcessor<K, V, Void> asProcessor =
        record -> {
          handler.handle(record);
          return Optional.empty();
        };
    return new ProcessorBinding<>(asProcessor, null, () -> {});
  }

  /**
   * Runs the processor.
   *
   * @param record the input record
   * @return the output, or empty if there is nothing to write
   * @throws Exception what the processor threw, or a {@link NullPointerException} if it returned
   *     null, which fails the attempt like any other exception
   */
  public Optional<O> process(ConsumerRecord<K, V> record) throws Exception {
    Optional<? extends O> output = processor.process(record);
    if (output == null) {
      throw new NullPointerException(
          "RecordProcessor returned null; return Optional.empty() when there is nothing to write");
    }
    return output.map(o -> o); // widens Optional<? extends O> to Optional<O>
  }

  /**
   * Writes an output. A sink that throws instead of returning a failed stage is treated as a failed
   * write.
   *
   * @param ctx identity of the input record
   * @param output the output
   * @return the write stage
   */
  public CompletionStage<Void> write(SinkContext ctx, O output) {
    try {
      CompletionStage<Void> stage = sink.write(ctx, output);
      return stage != null
          ? stage
          : CompletableFuture.failedFuture(
              new NullPointerException("RecordSink.write returned null"));
    } catch (Throwable t) {
      FatalErrors.rethrowIfJvmFatal(t);
      return CompletableFuture.failedFuture(t);
    }
  }

  /**
   * The sink, or null if this binds a handler.
   *
   * @return the sink
   */
  public RecordSink<O> sink() {
    return sink;
  }

  /**
   * What to close for the processing step at shutdown (the processor; nothing for a handler).
   *
   * @return the closeable
   */
  public AutoCloseable processorCloseable() {
    return owner;
  }
}
