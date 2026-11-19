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
import io.github.openlogiclab.kafkapipeline.internal.Classification;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.TopicPartition;

/**
 * Writes each element of a list with another sink; see {@link RecordSink#forEach(RecordSink)}.
 *
 * @param <E> element type
 */
final class ForEachSink<E> implements RecordSink<List<E>> {

  private final RecordSink<E> sink;

  ForEachSink(RecordSink<E> sink) {
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  @Override
  public CompletionStage<Void> write(SinkContext ctx, List<E> outputs) {
    if (outputs.isEmpty()) return CompletableFuture.completedFuture(null);
    // Before writing anything, so a bad list never leaves some of its elements written.
    for (int i = 0; i < outputs.size(); i++) {
      if (outputs.get(i) == null) {
        return CompletableFuture.failedFuture(
            new NullPointerException("element " + i + " of the processor's output is null"));
      }
    }
    CompletableFuture<?>[] writes = new CompletableFuture<?>[outputs.size()];
    for (int i = 0; i < writes.length; i++) {
      writes[i] = writeOne(ctx.forOutput(i), outputs.get(i));
    }
    // Completes once every element's write has completed, so a retry never overlaps a write of
    // this record still in flight.
    CompletableFuture<Void> result = new CompletableFuture<>();
    CompletableFuture.allOf(writes)
        .whenComplete(
            (ignored, error) -> {
              // Runs on the inner sink's thread, where CompletableFuture would swallow a throw and
              // leave the record's write pending forever: always complete the result.
              try {
                if (error == null) {
                  result.complete(null);
                } else {
                  result.completeExceptionally(mostSevereFailure(writes));
                }
              } catch (Throwable t) {
                result.completeExceptionally(t);
              }
            });
    return result;
  }

  private CompletableFuture<Void> writeOne(SinkContext ctx, E output) {
    try {
      CompletionStage<Void> stage = sink.write(ctx, output);
      if (stage == null) {
        return CompletableFuture.failedFuture(
            new NullPointerException("RecordSink.write returned null"));
      }
      // Bridged rather than toCompletableFuture(), which a stage is allowed not to support.
      CompletableFuture<Void> written = new CompletableFuture<>();
      stage.whenComplete(
          (ignored, error) -> {
            if (error == null) {
              written.complete(null);
            } else {
              written.completeExceptionally(error);
            }
          });
      return written;
    } catch (Throwable t) { // a sink that throws instead of returning a failed stage
      FatalErrors.rethrowIfJvmFatal(t);
      return CompletableFuture.failedFuture(t);
    }
  }

  /**
   * The failure that decides what happens to the record: the most severe by {@link #classify}, so
   * one element failing fatally stops the pipeline even if another element failed for a reason that
   * would only send the record to the DLQ. The others are attached as suppressed.
   */
  private Throwable mostSevereFailure(CompletableFuture<?>[] writes) {
    List<Throwable> failures = new ArrayList<>();
    for (CompletableFuture<?> w : writes) {
      try {
        w.join(); // every write has completed: never blocks
      } catch (CompletionException e) {
        failures.add(e.getCause() != null ? e.getCause() : e);
      } catch (CancellationException e) {
        failures.add(e);
      }
    }
    Throwable worst = failures.getFirst();
    int worstRank = rank(worst);
    for (Throwable t : failures) {
      int r = rank(t);
      if (r > worstRank) {
        worst = t;
        worstRank = r;
      }
    }
    for (Throwable t : failures) {
      if (t != worst) worst.addSuppressed(t);
    }
    return worst;
  }

  private int rank(Throwable failure) {
    return switch (Classification.classify(sink::classify, failure)) {
      case RECORD -> 1;
      case TRANSIENT -> 2;
      case FATAL -> 3;
    };
  }

  @Override
  public CompletionStage<Void> flush(TopicPartition partition) {
    return sink.flush(partition);
  }

  @Override
  public void onPartitionRevoked(TopicPartition partition, Duration budget) {
    sink.onPartitionRevoked(partition, budget);
  }

  @Override
  public Optional<BackpressureSensor> sensor() {
    return sink.sensor();
  }

  @Override
  public WriteFailure classify(Throwable error) {
    return sink.classify(error);
  }

  @Override
  public OptionalInt maxBufferedRecordsPerPartition() {
    return sink.maxBufferedRecordsPerPartition();
  }

  @Override
  public void close() throws Exception {
    sink.close();
  }
}
