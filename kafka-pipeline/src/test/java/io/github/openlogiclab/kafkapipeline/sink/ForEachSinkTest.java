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

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class ForEachSinkTest {

  private static final TopicPartition TP = new TopicPartition("in", 0);
  private static final SinkContext CTX = new SinkContext(TP, 7, new RecordHeaders());

  /**
   * Records what it is asked to write and leaves each write pending until the test completes it.
   */
  private static final class PendingSink implements RecordSink<String> {
    final List<String> written = new ArrayList<>();
    final List<CompletableFuture<Void>> stages = new ArrayList<>();

    @Override
    public CompletionStage<Void> write(SinkContext ctx, String output) {
      written.add(output);
      CompletableFuture<Void> stage = new CompletableFuture<>();
      stages.add(stage);
      return stage;
    }
  }

  @Test
  void writesEachElementInListOrder_andCompletesWhenAllAreDurable() throws Exception {
    PendingSink inner = new PendingSink();
    CompletableFuture<Void> stage =
        RecordSink.forEach(inner).write(CTX, List.of("a", "b", "c")).toCompletableFuture();

    assertEquals(List.of("a", "b", "c"), inner.written);
    inner.stages.get(0).complete(null);
    inner.stages.get(2).complete(null);
    assertFalse(stage.isDone(), "one element is not durable yet");
    inner.stages.get(1).complete(null);
    stage.get(1, TimeUnit.SECONDS);
  }

  @Test
  void emptyList_completesWithoutWriting() {
    PendingSink inner = new PendingSink();
    CompletionStage<Void> stage = RecordSink.forEach(inner).write(CTX, List.of());
    assertTrue(stage.toCompletableFuture().isDone());
    assertTrue(inner.written.isEmpty());
  }

  @Test
  void failedElement_failsTheWriteOnlyOnceEveryElementHasCompleted() {
    PendingSink inner = new PendingSink();
    CompletableFuture<Void> stage =
        RecordSink.forEach(inner).write(CTX, List.of("a", "b")).toCompletableFuture();

    inner.stages.get(1).completeExceptionally(new IllegalStateException("rejected"));
    assertFalse(stage.isDone(), "a retry must not overlap the write of a still in flight");
    inner.stages.get(0).complete(null);
    assertTrue(stage.isCompletedExceptionally());
  }

  @Test
  void nullElement_failsBeforeAnythingIsWritten() {
    PendingSink inner = new PendingSink();
    CompletableFuture<Void> stage =
        RecordSink.forEach(inner).write(CTX, Arrays.asList("a", null)).toCompletableFuture();
    assertTrue(stage.isCompletedExceptionally());
    assertTrue(inner.written.isEmpty(), "no partial output for a bad list");
  }

  @Test
  void eachElementIsWrittenWithItsOwnOutputIndex() {
    List<SinkContext> contexts = new ArrayList<>();
    RecordSink<String> recording =
        (ctx, output) -> {
          contexts.add(ctx);
          return CompletableFuture.completedFuture(null);
        };
    RecordSink.forEach(recording).write(CTX, List.of("a", "b"));
    assertEquals(List.of(0, 1), contexts.stream().map(SinkContext::outputIndex).toList());
    assertEquals(
        List.of("in-0-7-0", "in-0-7-1"), contexts.stream().map(SinkContext::outputId).toList());
    assertEquals(CTX.recordId(), contexts.get(1).recordId(), "outputs share their record's id");
  }

  @Test
  void failuresOfSeveralElements_failWithTheMostSevere() {
    RuntimeException badRecord = new IllegalArgumentException("record too large");
    RuntimeException fatal = new SecurityException("not authorized");
    RecordSink<String> inner =
        new RecordSink<>() {
          @Override
          public CompletionStage<Void> write(SinkContext ctx, String output) {
            return CompletableFuture.failedFuture(output.equals("fatal") ? fatal : badRecord);
          }

          @Override
          public WriteFailure classify(Throwable error) {
            return error == fatal ? WriteFailure.FATAL : WriteFailure.RECORD;
          }
        };
    CompletableFuture<Void> stage =
        RecordSink.forEach(inner).write(CTX, List.of("bad", "fatal")).toCompletableFuture();

    ExecutionException e = assertThrows(ExecutionException.class, stage::get);
    assertSame(fatal, e.getCause(), "a fatal element must stop the pipeline, not go to the DLQ");
    assertArrayEquals(new Throwable[] {badRecord}, fatal.getSuppressed());
  }

  @Test
  void elementSinkWhoseClassifyIsBroken_stillCompletesTheWrite() {
    for (boolean returnNull : new boolean[] {true, false}) {
      RecordSink<String> inner =
          new RecordSink<>() {
            @Override
            public CompletionStage<Void> write(SinkContext ctx, String output) {
              return CompletableFuture.failedFuture(new IllegalStateException("rejected"));
            }

            @Override
            public WriteFailure classify(Throwable error) {
              if (returnNull) return null;
              throw new AssertionError("broken classifier");
            }
          };
      CompletableFuture<Void> stage =
          RecordSink.forEach(inner).write(CTX, List.of("a", "b")).toCompletableFuture();
      assertTrue(
          stage.isCompletedExceptionally(), "the write must complete, returnNull=" + returnNull);
    }
  }

  @Test
  void cancelledElement_failsTheWrite() {
    PendingSink inner = new PendingSink();
    CompletableFuture<Void> stage =
        RecordSink.forEach(inner).write(CTX, List.of("a", "b")).toCompletableFuture();
    inner.stages.get(0).cancel(false);
    inner.stages.get(1).complete(null);
    assertTrue(stage.isCompletedExceptionally());
  }

  @Test
  void stageWithoutToCompletableFuture_isSupported() throws Exception {
    RecordSink<String> minimal =
        (ctx, output) -> CompletableFuture.<Void>completedFuture(null).minimalCompletionStage();
    RecordSink.forEach(minimal)
        .write(CTX, List.of("a", "b"))
        .toCompletableFuture()
        .get(1, TimeUnit.SECONDS);
  }

  @Test
  void outputIndex_isNotNegative() {
    assertThrows(IllegalArgumentException.class, () -> CTX.forOutput(-1));
    assertEquals(0, CTX.outputIndex());
  }

  @Test
  void sinkThatThrowsOrReturnsNull_failsTheWriteAfterTheOtherElements() {
    RecordSink<String> throwing =
        (ctx, output) -> {
          if (output.equals("throw")) throw new IllegalStateException("threw");
          return output.equals("null") ? null : CompletableFuture.completedFuture(null);
        };
    assertTrue(
        RecordSink.forEach(throwing)
            .write(CTX, List.of("ok", "throw"))
            .toCompletableFuture()
            .isCompletedExceptionally());
    assertTrue(
        RecordSink.forEach(throwing)
            .write(CTX, List.of("null", "ok"))
            .toCompletableFuture()
            .isCompletedExceptionally());
  }

  @Test
  void everythingButWrite_isTheInnerSinks() throws Exception {
    BackpressureSensor sensor = BackpressureSensor.watermark("test", () -> 0, 10, 5);
    CompletableFuture<Void> flushed = new CompletableFuture<>();
    AtomicBoolean revoked = new AtomicBoolean();
    AtomicBoolean closed = new AtomicBoolean();
    RecordSink<String> inner =
        new RecordSink<>() {
          @Override
          public CompletionStage<Void> write(SinkContext ctx, String output) {
            return CompletableFuture.completedFuture(null);
          }

          @Override
          public CompletionStage<Void> flush(TopicPartition partition) {
            return flushed;
          }

          @Override
          public void onPartitionRevoked(TopicPartition partition, Duration budget) {
            revoked.set(true);
          }

          @Override
          public Optional<BackpressureSensor> sensor() {
            return Optional.of(sensor);
          }

          @Override
          public WriteFailure classify(Throwable error) {
            return WriteFailure.FATAL;
          }

          @Override
          public OptionalInt maxBufferedRecordsPerPartition() {
            return OptionalInt.of(5);
          }

          @Override
          public void close() {
            closed.set(true);
          }
        };
    RecordSink<List<String>> lists = RecordSink.forEach(inner);

    assertSame(flushed, lists.flush(TP));
    lists.onPartitionRevoked(TP, Duration.ZERO);
    assertTrue(revoked.get());
    assertSame(sensor, lists.sensor().orElseThrow());
    assertEquals(WriteFailure.FATAL, lists.classify(new RuntimeException()));
    assertEquals(OptionalInt.of(5), lists.maxBufferedRecordsPerPartition());
    lists.close();
    assertTrue(closed.get());
  }
}
