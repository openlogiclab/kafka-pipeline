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
package io.github.openlogiclab.kafkapipeline.worker;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.ProcessingOrder;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.internal.HandlerAttemptObserver;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
import io.github.openlogiclab.kafkapipeline.internal.ProcessorBinding;
import io.github.openlogiclab.kafkapipeline.internal.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.offset.SlidingWindowOffsetTracker;
import io.github.openlogiclab.kafkapipeline.sink.RecordProcessor;
import io.github.openlogiclab.kafkapipeline.sink.RecordSink;
import io.github.openlogiclab.kafkapipeline.sink.SinkContext;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Records processed into a sink are acked only after their write completes. */
class SinkWorkerPoolTest {

  private static final TopicPartition TP0 = new TopicPartition("in", 0);

  private SlidingWindowOffsetTracker tracker;
  private RecordDispatcher<String, String> dispatcher;
  private InFlightCounter counter;
  private SingleRecordWorkerPool<String, String> pool;

  /** A sink whose writes complete only when the test says so. */
  static final class ManualSink implements RecordSink<String> {
    final List<String> written = new CopyOnWriteArrayList<>();
    final List<CompletableFuture<Void>> stages = new CopyOnWriteArrayList<>();
    final List<SinkContext> contexts = new CopyOnWriteArrayList<>();
    final java.util.Map<String, CompletableFuture<Void>> byOutput =
        new java.util.concurrent.ConcurrentHashMap<>();
    final AtomicInteger closed = new AtomicInteger();

    @Override
    public synchronized CompletableFuture<Void> write(SinkContext ctx, String output) {
      CompletableFuture<Void> stage = new CompletableFuture<>();
      contexts.add(ctx);
      written.add(output);
      byOutput.put(output, stage);
      stages.add(stage);
      return stage;
    }

    void completeAll() {
      stages.forEach(s -> s.complete(null));
    }

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  @AfterEach
  void tearDown() {
    if (pool != null) pool.stop(2000);
  }

  private void setUp(ProcessingOrder order, int concurrency) {
    tracker = new SlidingWindowOffsetTracker();
    tracker.initPartition(TP0, 0);
    dispatcher = new RecordDispatcher<>(100, 0, concurrency, order);
    dispatcher.addPartition(TP0, tracker.generation(TP0));
    counter = new InFlightCounter();
  }

  private SingleRecordWorkerPool<String, String> pool(
      int concurrency,
      ErrorStrategy<String, String> strategy,
      RecordProcessor<String, String, String> processor,
      RecordSink<String> sink) {
    return pool(concurrency, strategy, processor, sink, Duration.ofMinutes(5));
  }

  private SingleRecordWorkerPool<String, String> pool(
      int concurrency,
      ErrorStrategy<String, String> strategy,
      RecordProcessor<String, String, String> processor,
      RecordSink<String> sink,
      Duration writeOutageTimeout) {
    return new SingleRecordWorkerPool<>(
        concurrency,
        ThreadMode.VIRTUAL,
        ProcessorBinding.of(processor, sink),
        ProcessingLifecycleHook.noOp(),
        new RetryExecutor<>(
            strategy,
            NoOpMetricsCollector.INSTANCE,
            HandlerAttemptObserver.NONE,
            Duration.ZERO,
            writeOutageTimeout),
        tracker,
        dispatcher,
        counter,
        NoOpMetricsCollector.INSTANCE);
  }

  private void dispatch(long offset, String key) {
    tracker.register(TP0, offset);
    counter.registered(1, 0);
    dispatcher.dispatch(TP0, new ConsumerRecord<>("in", 0, offset, key, "v" + offset));
    dispatcher.wakeWorkers(1);
  }

  private static boolean await(BooleanSupplier cond) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) return false;
      Thread.sleep(5);
    }
    return true;
  }

  private long committable() {
    OptionalLong c = tracker.getCommittableOffset(TP0);
    return c.isPresent() ? c.getAsLong() : 0;
  }

  @Nested
  class AckAfterWrite {

    @Test
    void recordIsAckedOnlyWhenItsWriteCompletes() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      ManualSink sink = new ManualSink();
      pool = pool(1, ErrorStrategy.dropOnError(), r -> Optional.of("out-" + r.offset()), sink);
      pool.start();
      dispatch(0, "k");

      assertTrue(await(() -> sink.stages.size() == 1));
      Thread.sleep(50);
      assertEquals(0, committable(), "not committable while the write is in flight");
      assertEquals(1, counter.records(), "still in flight for backpressure");
      assertEquals(1, pool.pendingWrites());

      sink.completeAll();
      assertTrue(await(() -> committable() == 1));
      // The slot is released right after the ack, on the outcome thread.
      assertTrue(await(() -> counter.records() == 0));
      assertEquals(List.of("out-0"), sink.written);
      assertEquals("in-0-0", sink.contexts.get(0).recordId());
    }

    @Test
    void workerIsFreeWhileWritesAreInFlight() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      ManualSink sink = new ManualSink();
      pool = pool(1, ErrorStrategy.dropOnError(), r -> Optional.of("out-" + r.offset()), sink);
      pool.start();
      for (long o = 0; o < 5; o++) dispatch(o, "k" + o);

      // One worker, five writes in flight at once: it did not wait for any of them.
      assertTrue(await(() -> sink.stages.size() == 5));
      sink.completeAll();
      assertTrue(await(() -> committable() == 5));
    }

    @Test
    void emptyOutput_acksWithoutWriting() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      ManualSink sink = new ManualSink();
      pool = pool(1, ErrorStrategy.dropOnError(), r -> Optional.empty(), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1));
      assertTrue(sink.stages.isEmpty());
    }

    @Test
    void nullReturn_isAProcessingFailure() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicReference<Exception> seen = new AtomicReference<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .fallback(Fallback.DROP)
              .onFinalFailure((r, e) -> seen.set(e))
              .build();
      pool = pool(1, strategy, r -> null, new ManualSink());
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1), "dropped, so the partition moves on");
      assertInstanceOf(NullPointerException.class, seen.get());
    }
  }

  @Nested
  class Ordering {

    @Test
    void keyOrder_nextRecordOfAKeyWaitsForThePreviousWrite() throws Exception {
      setUp(ProcessingOrder.KEY, 4);
      ManualSink sink = new ManualSink();
      List<Long> processed = new CopyOnWriteArrayList<>();
      pool =
          pool(
              4,
              ErrorStrategy.dropOnError(),
              r -> {
                processed.add(r.offset());
                return Optional.of(r.key() + "@" + r.offset());
              },
              sink);
      pool.start();
      dispatch(0, "A");
      dispatch(1, "A");
      dispatch(2, "B");

      // A@0 and B@2 run in parallel; A@1 must wait until A@0 is written, not just processed.
      assertTrue(await(() -> sink.stages.size() == 2));
      Thread.sleep(50);
      assertFalse(processed.contains(1L), "A@1 started before A@0's write completed");

      sink.byOutput.get("A@0").complete(null);
      assertTrue(await(() -> processed.contains(1L)));
      assertTrue(await(() -> sink.stages.size() == 3));
      int a0 = sink.written.indexOf("A@0");
      int a1 = sink.written.indexOf("A@1");
      assertTrue(a0 >= 0 && a0 < a1, "per-key output order: " + sink.written);
      sink.completeAll();
      assertTrue(await(() -> committable() == 3));
    }

    @Test
    void partitionOrder_oneWriteInFlightPerPartition() throws Exception {
      setUp(ProcessingOrder.PARTITION, 4);
      ManualSink sink = new ManualSink();
      pool = pool(4, ErrorStrategy.dropOnError(), r -> Optional.of("o" + r.offset()), sink);
      pool.start();
      for (long o = 0; o < 3; o++) dispatch(o, "k" + o);

      for (int i = 1; i <= 3; i++) {
        int expected = i;
        assertTrue(await(() -> sink.stages.size() == expected));
        Thread.sleep(30);
        assertEquals(expected, sink.stages.size(), "only one write in flight");
        sink.stages.get(i - 1).complete(null);
      }
      assertEquals(List.of("o0", "o1", "o2"), sink.written);
      assertTrue(await(() -> committable() == 3));
    }
  }

  @Nested
  class WriteFailures {

    @Test
    void failedWrite_isRetriedWithoutReprocessing() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger processorCalls = new AtomicInteger();
      AtomicInteger writes = new AtomicInteger();
      RecordSink<String> flaky =
          (ctx, output) ->
              writes.incrementAndGet() < 3
                  ? CompletableFuture.failedFuture(new IllegalStateException("broker down"))
                  : CompletableFuture.completedFuture(null);
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder().maxRetries(3).retryBackoff(Duration.ZERO).build();
      pool =
          pool(
              1,
              strategy,
              r -> {
                processorCalls.incrementAndGet();
                return Optional.of("x");
              },
              flaky);
      pool.start();
      dispatch(0, "k");

      assertTrue(await(() -> committable() == 1));
      assertEquals(3, writes.get());
      assertEquals(1, processorCalls.get(), "a write retry must not re-run the processor");
    }

    @Test
    void writeThatKeepsFailing_goesToTheDlqAndIsAcked() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(1)
              .retryBackoff(Duration.ZERO)
              .dlqHandler((r, e) -> dlq.add(r.offset()))
              .build();
      pool =
          pool(
              1,
              strategy,
              r -> Optional.of("x"),
              (ctx, output) -> {
                throw new IllegalStateException("sink threw instead of failing the stage");
              });
      pool.start();
      dispatch(0, "k");

      assertTrue(await(() -> committable() == 1), "a bad write never blocks the partition");
      assertEquals(List.of(0L), dlq);
      assertTrue(await(() -> counter.records() == 0));
    }

    @Test
    void afterProcessRunsOnlyAfterTheWrite() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      ManualSink sink = new ManualSink();
      List<Long> after = new CopyOnWriteArrayList<>();
      pool =
          new SingleRecordWorkerPool<>(
              1,
              ThreadMode.VIRTUAL,
              ProcessorBinding.of(
                  (RecordProcessor<String, String, String>) r -> Optional.of("x"), sink),
              new ProcessingLifecycleHook<>() {
                @Override
                public void afterProcess(ConsumerRecord<String, String> r, ProcessingContext c) {
                  after.add(r.offset());
                }
              },
              new RetryExecutor<>(ErrorStrategy.dropOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              dispatcher,
              counter,
              NoOpMetricsCollector.INSTANCE);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> sink.stages.size() == 1));
      Thread.sleep(30);
      assertTrue(after.isEmpty());
      sink.completeAll();
      assertTrue(await(() -> after.equals(List.of(0L))));
    }
  }

  @Nested
  class WriteFailureModel {

    private ErrorStrategy<String, String> noRetriesWithDlq(List<Long> dlq) {
      return ErrorStrategy.<String, String>builder()
          .maxRetries(0)
          .retryBackoff(Duration.ZERO)
          .dlqHandler((r, e) -> dlq.add(r.offset()))
          .build();
    }

    @Test
    void fatalWriteError_stopsThePipelineAndCommitsNothing() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      AtomicReference<Throwable> fatal = new AtomicReference<>();
      pool =
          pool(
              1,
              noRetriesWithDlq(dlq),
              r -> Optional.of("x"),
              (ctx, out) ->
                  CompletableFuture.failedFuture(
                      new org.apache.kafka.common.errors.TopicAuthorizationException("no ACL")));
      pool.onFatalError(fatal::set);
      pool.start();
      dispatch(0, "k");

      assertTrue(await(() -> fatal.get() != null), "a missing ACL stops the pipeline");
      assertTrue(await(() -> counter.records() == 0), "slots are still released");
      assertEquals(0, committable(), "never commit input whose output was not written");
      assertTrue(dlq.isEmpty(), "not a bad record: not dead-lettered");
    }

    @Test
    void transientWriteError_isRetriedBeyondMaxRetriesUntilItSucceeds() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      AtomicInteger writes = new AtomicInteger();
      pool =
          pool(
              1,
              noRetriesWithDlq(dlq),
              r -> Optional.of("x"),
              (ctx, out) ->
                  writes.incrementAndGet() <= 3
                      ? CompletableFuture.failedFuture(
                          new org.apache.kafka.common.errors.TimeoutException("broker down"))
                      : CompletableFuture.completedFuture(null));
      pool.start();
      dispatch(0, "k");

      assertTrue(await(() -> committable() == 1), "written once the destination recovered");
      assertEquals(4, writes.get(), "transient failures do not use the retry budget");
      assertTrue(dlq.isEmpty(), "an outage is not a bad record");
    }

    @Test
    void customTransientClassification_isHonoured() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger writes = new AtomicInteger();
      RecordSink<String> sink =
          new RecordSink<>() {
            @Override
            public CompletableFuture<Void> write(SinkContext ctx, String output) {
              return writes.incrementAndGet() <= 2
                  ? CompletableFuture.failedFuture(new java.sql.SQLTransientConnectionException())
                  : CompletableFuture.completedFuture(null);
            }

            @Override
            public WriteFailure classify(Throwable error) {
              return error instanceof java.sql.SQLTransientException
                  ? WriteFailure.TRANSIENT
                  : WriteFailure.RECORD;
            }
          };
      pool = pool(1, noRetriesWithDlq(new CopyOnWriteArrayList<>()), r -> Optional.of("x"), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1));
      assertEquals(3, writes.get());
    }

    @Test
    void sinkClassifyThatThrowsAnError_failsTheRecordInsteadOfHangingIt() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      RecordSink<String> sink =
          new RecordSink<>() {
            @Override
            public CompletableFuture<Void> write(SinkContext ctx, String output) {
              return CompletableFuture.failedFuture(new IllegalStateException("rejected"));
            }

            @Override
            public WriteFailure classify(Throwable error) {
              throw new AssertionError("broken classifier");
            }
          };
      pool = pool(1, noRetriesWithDlq(dlq), r -> Optional.of("x"), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1), "classified as RECORD: dead-lettered and acked");
      assertEquals(List.of(0L), dlq);
      assertTrue(await(() -> counter.records() == 0));
    }

    @Test
    void sinkClassifyReturningNull_isARecordFailure() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      RecordSink<String> sink =
          new RecordSink<>() {
            @Override
            public CompletableFuture<Void> write(SinkContext ctx, String output) {
              return CompletableFuture.failedFuture(new IllegalStateException("rejected"));
            }

            @Override
            public WriteFailure classify(Throwable error) {
              return null;
            }
          };
      pool = pool(1, noRetriesWithDlq(dlq), r -> Optional.of("x"), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1));
      assertEquals(List.of(0L), dlq);
    }

    @Test
    void writeOfARevokedPartition_isAbandonedNotRetriedOrDeadLettered() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      AtomicInteger writes = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(5)
              .retryBackoff(Duration.ofSeconds(2)) // wide margin: the revoke lands inside it
              .dlqHandler((r, e) -> dlq.add(r.offset()))
              .build();
      pool =
          pool(
              1,
              strategy,
              r -> Optional.of("x"),
              (ctx, out) -> {
                writes.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("bad"));
              });
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> writes.get() == 1)); // now waiting out the backoff

      // The partition is revoked and assigned back: a new assignment, a new generation.
      tracker.clearPartition(TP0);
      tracker.initPartition(TP0, 0);

      assertTrue(await(() -> counter.records() == 0), "abandoned once the backoff ends");
      assertEquals(1, writes.get(), "no retry for an old assignment");
      assertTrue(dlq.isEmpty(), "no DLQ for a record the new owner processes");
    }

    @Test
    void transientWriteOfARevokedPartition_isAbandoned() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger writes = new AtomicInteger();
      pool =
          pool(
              1,
              ErrorStrategy.<String, String>builder().retryBackoff(Duration.ofSeconds(2)).build(),
              r -> Optional.of("x"),
              (ctx, out) -> {
                writes.incrementAndGet();
                return CompletableFuture.failedFuture(
                    new org.apache.kafka.common.errors.TimeoutException("broker down"));
              });
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> writes.get() == 1));
      tracker.clearPartition(TP0);
      tracker.initPartition(TP0, 0);
      assertTrue(await(() -> counter.records() == 0), "a transient retry stops for a revoke");
      assertEquals(1, writes.get());
    }

    @Test
    void transientWriteAtShutdown_isAbandonedAtOnce() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger writes = new AtomicInteger();
      pool =
          pool(
              1,
              ErrorStrategy.<String, String>builder().retryBackoff(Duration.ofSeconds(30)).build(),
              r -> Optional.of("x"),
              (ctx, out) -> {
                writes.incrementAndGet();
                return CompletableFuture.failedFuture(
                    new org.apache.kafka.common.errors.TimeoutException("broker down"));
              });
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> writes.get() == 1));
      long start = System.nanoTime();
      pool.stop(10_000);
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "no backoff wait");
      assertEquals(0, committable());
      assertEquals(0, counter.records());
      pool = null;
    }

    @Test
    void outageLongerThanTheTimeout_stopsThePipelineWithoutDropping() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dlq = new CopyOnWriteArrayList<>();
      AtomicReference<Throwable> fatal = new AtomicReference<>();
      pool =
          pool(
              1,
              noRetriesWithDlq(dlq),
              r -> Optional.of("x"),
              (ctx, out) ->
                  CompletableFuture.failedFuture(
                      new org.apache.kafka.common.errors.TimeoutException("topic does not exist")),
              Duration.ofMillis(300));
      pool.onFatalError(fatal::set);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> fatal.get() != null), "a destination that never recovers stops it");
      assertTrue(fatal.get().getMessage().contains("unavailable"));
      assertTrue(await(() -> counter.records() == 0));
      assertEquals(0, committable(), "not dropped, not committed");
      assertTrue(dlq.isEmpty());
    }

    @Test
    void writesOfADrainingPartition_areFlushedAtOnce() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger flushes = new AtomicInteger();
      RecordSink<String> sink =
          new RecordSink<>() {
            @Override
            public CompletableFuture<Void> write(SinkContext ctx, String output) {
              return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> flush(TopicPartition partition) {
              flushes.incrementAndGet();
              return CompletableFuture.completedFuture(null);
            }
          };
      pool = pool(1, ErrorStrategy.dropOnError(), r -> Optional.of("x"), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1));
      assertEquals(0, flushes.get(), "normal writes are not flushed one by one");

      pool.onPartitionsDraining(List.of(TP0));
      dispatch(1, "k");
      assertTrue(await(() -> committable() == 2));
      assertEquals(1, flushes.get(), "a write of a draining partition is flushed");

      pool.onPartitionsDrained(List.of(TP0));
      dispatch(2, "k");
      assertTrue(await(() -> committable() == 3));
      assertEquals(1, flushes.get());
    }
  }

  @Test
  void processorRetryBackoff_endsAtOnceOnShutdown() throws Exception {
    setUp(ProcessingOrder.UNORDERED, 1);
    AtomicInteger attempts = new AtomicInteger();
    pool =
        pool(
            1,
            ErrorStrategy.<String, String>builder()
                .maxRetries(5)
                .retryBackoff(Duration.ofSeconds(30))
                .build(),
            r -> {
              attempts.incrementAndGet();
              throw new IllegalStateException("downstream down");
            },
            (ctx, out) -> CompletableFuture.completedFuture(null));
    pool.start();
    dispatch(0, "k");
    assertTrue(await(() -> attempts.get() == 1)); // now in a 30 s backoff
    long start = System.nanoTime();
    pool.stop(10_000);
    assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "no backoff wait");
    assertEquals(0, committable(), "left for redelivery");
    assertEquals(0, counter.records());
    pool = null;
  }

  @Nested
  class DlqFailureModel {

    private RecordSink<String> failingSink() {
      return (ctx, out) -> CompletableFuture.failedFuture(new IllegalArgumentException("bad"));
    }

    @Test
    void transientDlqFailure_isRetriedUntilTheDlqRecovers() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger sends = new AtomicInteger();
      List<Long> dropped = new CopyOnWriteArrayList<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ZERO)
              .dlqHandler(
                  (r, e) -> {
                    if (sends.incrementAndGet() <= 2) {
                      throw new org.apache.kafka.common.errors.TimeoutException("DLQ down");
                    }
                  })
              .onFinalFailure((r, e) -> dropped.add(r.offset()))
              .build();
      pool = pool(1, strategy, r -> Optional.of("x"), failingSink());
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1));
      assertEquals(3, sends.get(), "retried while the DLQ was down");
      assertTrue(dropped.isEmpty(), "never dropped because the DLQ was briefly down");
    }

    @Test
    void fatalDlqFailure_stopsThePipelineAndKeepsTheRecord() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicReference<Throwable> fatal = new AtomicReference<>();
      List<Long> dropped = new CopyOnWriteArrayList<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (r, e) -> {
                    throw new org.apache.kafka.common.errors.TopicAuthorizationException("no ACL");
                  })
              .onFinalFailure((r, e) -> dropped.add(r.offset()))
              .build();
      pool = pool(1, strategy, r -> Optional.of("x"), failingSink());
      pool.onFatalError(fatal::set);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> fatal.get() != null));
      assertTrue(await(() -> counter.records() == 0));
      assertEquals(0, committable(), "not dropped, not committed");
      assertTrue(dropped.isEmpty());
    }

    @Test
    void transientDlqFailureOfARevokedPartition_isAbandoned() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger sends = new AtomicInteger();
      List<Long> dropped = new CopyOnWriteArrayList<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofSeconds(20)) // long: only the revoke can end the wait
              .dlqHandler(
                  (r, e) -> {
                    sends.incrementAndGet();
                    throw new org.apache.kafka.common.errors.TimeoutException("DLQ down");
                  })
              .onFinalFailure((r, e) -> dropped.add(r.offset()))
              .build();
      pool = pool(1, strategy, r -> Optional.of("x"), failingSink());
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> sends.get() == 1));

      tracker.clearPartition(TP0);
      tracker.initPartition(TP0, 0);
      pool.onPartitionsDrained(List.of(TP0)); // what the rebalance listener does after a revoke

      assertTrue(await(() -> counter.records() == 0), "the DLQ retry gives up at once");
      assertEquals(1, sends.get(), "the old owner never dead-letters the new owner's record");
      assertTrue(dropped.isEmpty());
    }

    @Test
    void dlqOutageLongerThanTheTimeout_stopsThePipeline() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicReference<Throwable> fatal = new AtomicReference<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ZERO)
              .dlqHandler(
                  (r, e) -> {
                    throw new org.apache.kafka.common.errors.TimeoutException("DLQ down");
                  })
              .build();
      pool = pool(1, strategy, r -> Optional.of("x"), failingSink(), Duration.ofMillis(300));
      pool.onFatalError(fatal::set);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> fatal.get() != null));
      assertTrue(fatal.get().getMessage().contains("DLQ unavailable"));
      assertTrue(await(() -> counter.records() == 0));
      assertEquals(0, committable(), "not dropped, not committed");
    }

    @Test
    void dlqClassifyThatThrowsAnError_dropsTheRecordInsteadOfLeavingItUnacked() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dropped = new CopyOnWriteArrayList<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  new io.github.openlogiclab.kafkapipeline.error.DLQHandler<>() {
                    @Override
                    public void send(
                        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record,
                        Exception lastError) {
                      throw new IllegalStateException("DLQ rejected it");
                    }

                    @Override
                    public WriteFailure classify(Throwable error) {
                      throw new AssertionError("broken classifier");
                    }
                  })
              .onFinalFailure((r, e) -> dropped.add(r.offset()))
              .build();
      pool = pool(1, strategy, r -> Optional.of("x"), failingSink());
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1), "classified as RECORD: dropped and acked");
      assertEquals(List.of(0L), dropped);
    }

    @Test
    void recordSpecificDlqFailure_dropsTheRecord() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      List<Long> dropped = new CopyOnWriteArrayList<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (r, e) -> {
                    throw new org.apache.kafka.common.errors.RecordTooLargeException();
                  })
              .onFinalFailure((r, e) -> dropped.add(r.offset()))
              .build();
      pool = pool(1, strategy, r -> Optional.of("x"), failingSink());
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> committable() == 1), "a record the DLQ rejects is dropped");
      assertEquals(List.of(0L), dropped);
    }
  }

  @Nested
  class Shutdown {

    @Test
    void retryBackoff_isCutShortByShutdown() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger writes = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(5)
              .retryBackoff(Duration.ofSeconds(30))
              .build();
      pool =
          pool(
              1,
              strategy,
              r -> Optional.of("x"),
              (ctx, out) -> {
                writes.incrementAndGet();
                return CompletableFuture.failedFuture(new IllegalStateException("down"));
              });
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> writes.get() == 1)); // now waiting out a 30 s backoff

      long start = System.nanoTime();
      pool.stop(10_000);
      assertTrue(
          System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3),
          "shutdown must not wait for the backoff");
      assertEquals(0, committable(), "given up at shutdown: redelivered, not committed");
      assertEquals(0, counter.records());
      pool = null;
    }

    @Test
    void dlqSendInterruptedByShutdown_leavesTheRecordUnacked() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicInteger finalFailures = new AtomicInteger();
      java.util.concurrent.CountDownLatch inDlq = new java.util.concurrent.CountDownLatch(1);
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .dlqHandler(
                  (r, e) -> {
                    inDlq.countDown();
                    Thread.sleep(60_000); // a DLQ broker that does not answer
                  })
              .onFinalFailure((r, e) -> finalFailures.incrementAndGet())
              .build();
      pool =
          pool(
              1,
              strategy,
              r -> Optional.of("x"),
              (ctx, out) -> CompletableFuture.failedFuture(new IllegalStateException("bad")));
      pool.start();
      dispatch(0, "k");
      assertTrue(inDlq.await(5, TimeUnit.SECONDS));

      pool.stop(200); // gives up on the DLQ send and interrupts it
      assertTrue(await(() -> counter.records() == 0));
      assertEquals(0, committable(), "neither dead-lettered nor dropped, so not committed");
      assertEquals(0, finalFailures.get(), "a shutdown is not a final failure");
      pool = null;
    }

    @Test
    void fatalErrorInAWriteOutcome_stopsThePipeline() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      AtomicReference<Throwable> fatal = new AtomicReference<>();
      pool =
          new SingleRecordWorkerPool<>(
              1,
              ThreadMode.VIRTUAL,
              ProcessorBinding.of(
                  (RecordProcessor<String, String, String>) r -> Optional.of("x"),
                  (ctx, out) -> CompletableFuture.completedFuture(null)),
              new ProcessingLifecycleHook<>() {
                @Override
                public void afterProcess(ConsumerRecord<String, String> r, ProcessingContext c) {
                  throw new OutOfMemoryError("simulated");
                }
              },
              new RetryExecutor<>(ErrorStrategy.dropOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              dispatcher,
              counter,
              NoOpMetricsCollector.INSTANCE);
      pool.onFatalError(fatal::set);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> fatal.get() != null), "escalated like on a worker");
      assertInstanceOf(OutOfMemoryError.class, fatal.get());
      assertEquals(0, committable(), "not acked: redelivered after restart");
      assertTrue(await(() -> counter.records() == 0));
    }

    @Test
    void writesStillPendingAtShutdown_areNotAckedButReleased() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      ManualSink sink = new ManualSink();
      pool = pool(1, ErrorStrategy.dropOnError(), r -> Optional.of("x"), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> sink.stages.size() == 1));

      long start = System.nanoTime();
      pool.stop(200);
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2), "bounded by timeout");
      assertEquals(0, committable(), "an unwritten record is not committed");

      sink.stages.get(0).completeExceptionally(new IllegalStateException("closed"));
      assertTrue(await(() -> counter.records() == 0), "a late outcome still releases the slot");
      pool = null;
    }

    @Test
    void writesThatCompleteDuringShutdown_areAcked() throws Exception {
      setUp(ProcessingOrder.UNORDERED, 1);
      ManualSink sink = new ManualSink();
      pool = pool(1, ErrorStrategy.dropOnError(), r -> Optional.of("x"), sink);
      pool.start();
      dispatch(0, "k");
      assertTrue(await(() -> sink.stages.size() == 1));

      Thread completer =
          Thread.startVirtualThread(
              () -> {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  return;
                }
                sink.completeAll();
              });
      pool.stop(3000);
      completer.join();
      assertEquals(1, committable(), "written before the deadline, so committed");
      pool = null;
    }
  }
}
