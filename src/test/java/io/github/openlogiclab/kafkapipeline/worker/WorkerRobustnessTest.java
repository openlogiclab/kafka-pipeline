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
import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.internal.HandlerAttemptObserver;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for worker failure modes that used to stall a partition silently: Errors thrown
 * by handlers, interrupts left by handlers, hung handlers, and a dispatch that failed partway.
 */
class WorkerRobustnessTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);

  private UnorderedOffsetTracker tracker;
  private RecordDispatcher<String, String> dispatcher;
  private InFlightCounter counter;
  private WorkerPool<String, String> pool;

  @BeforeEach
  void setUp() {
    tracker = new UnorderedOffsetTracker();
    tracker.initPartition(TP0, 0);
    dispatcher = new RecordDispatcher<>(100);
    dispatcher.addPartition(TP0);
    counter = new InFlightCounter();
  }

  @AfterEach
  void tearDown() {
    if (pool != null) pool.stop(2000);
  }

  private void registerAndDispatch(long offset) {
    tracker.register(TP0, offset);
    counter.registered(1, 0);
    dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, offset, "k", "v-" + offset));
    dispatcher.wakeWorkers(1);
  }

  private SingleRecordWorkerPool<String, String> singlePool(
      ErrorStrategy<String, String> strategy,
      Duration processingTimeout,
      RecordHandler<String, String> handler,
      ProcessingLifecycleHook<String, String> hook) {
    return new SingleRecordWorkerPool<>(
        1,
        ThreadMode.PLATFORM,
        handler,
        hook,
        new RetryExecutor<>(
            strategy,
            NoOpMetricsCollector.INSTANCE,
            HandlerAttemptObserver.NONE,
            processingTimeout),
        tracker,
        dispatcher,
        counter,
        NoOpMetricsCollector.INSTANCE);
  }

  private static boolean await(BooleanSupplier cond, long millis) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (!cond.getAsBoolean()) {
      if (System.nanoTime() > deadline) return false;
      Thread.sleep(10);
    }
    return true;
  }

  private boolean committedTo(long offset) throws InterruptedException {
    return await(
        () -> {
          OptionalLong c = tracker.getCommittableOffset(TP0);
          return c.isPresent() && c.getAsLong() >= offset;
        },
        5000);
  }

  @Nested
  class HandlerErrors {

    @Test
    void errorFromHandler_isSkippedAndWorkerSurvives() throws Exception {
      List<Long> processed = new CopyOnWriteArrayList<>();
      pool =
          singlePool(
              ErrorStrategy.skipOnError(),
              Duration.ZERO,
              r -> {
                if (r.offset() == 0) throw new AssertionError("boom");
                processed.add(r.offset());
              },
              ProcessingLifecycleHook.noOp());
      pool.start();
      registerAndDispatch(0);
      registerAndDispatch(1);

      // The single worker survived the Error, skipped offset 0 and processed offset 1.
      assertTrue(committedTo(2), "window must move past the record whose handler threw an Error");
      assertEquals(List.of(1L), processed);
      assertEquals(0, counter.records());
    }

    @Test
    void errorReachesFinalFailureHandlerWrapped() throws Exception {
      AtomicReference<Exception> seen = new AtomicReference<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .fallback(Fallback.SKIP)
              .onFinalFailure((r, e) -> seen.set(e))
              .build();
      pool =
          singlePool(
              strategy,
              Duration.ZERO,
              r -> {
                throw new StackOverflowError("deep");
              },
              ProcessingLifecycleHook.noOp());
      pool.start();
      registerAndDispatch(0);

      assertTrue(committedTo(1));
      assertInstanceOf(StackOverflowError.class, seen.get().getCause());
    }

    @Test
    void virtualMachineError_isFatalAndRecordStaysUnacked() throws Exception {
      AtomicReference<Throwable> fatal = new AtomicReference<>();
      pool =
          singlePool(
              ErrorStrategy.skipOnError(),
              Duration.ZERO,
              r -> {
                throw new OutOfMemoryError("simulated");
              },
              ProcessingLifecycleHook.noOp());
      pool.onFatalError(fatal::set);
      pool.start();
      registerAndDispatch(0);

      assertTrue(await(() -> fatal.get() != null, 5000), "fatal error must be escalated");
      assertInstanceOf(OutOfMemoryError.class, fatal.get());
      // Not skipped: the record is redelivered after the pipeline restarts.
      assertTrue(tracker.getCommittableOffset(TP0).isEmpty());
      assertEquals(0, counter.records(), "in-flight slot is still released");
    }

    @Test
    void onErrorHookThatThrows_doesNotHideHandlerException() throws Exception {
      AtomicReference<Exception> seen = new AtomicReference<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .fallback(Fallback.SKIP)
              .onFinalFailure((r, e) -> seen.set(e))
              .build();
      ProcessingLifecycleHook<String, String> hook =
          new ProcessingLifecycleHook<>() {
            @Override
            public void onError(
                ConsumerRecord<String, String> record, ProcessingContext ctx, Exception error) {
              throw new IllegalStateException("hook failed");
            }
          };
      pool =
          singlePool(
              strategy,
              Duration.ZERO,
              r -> {
                throw new IllegalArgumentException("handler failed");
              },
              hook);
      pool.start();
      registerAndDispatch(0);

      assertTrue(committedTo(1));
      assertEquals("handler failed", seen.get().getMessage());
      assertEquals("hook failed", seen.get().getSuppressed()[0].getMessage());
    }
  }

  @Nested
  class Interrupts {

    @Test
    void handlerThatRestoresInterrupt_doesNotEndTheWorker() throws Exception {
      List<Long> processed = new CopyOnWriteArrayList<>();
      pool =
          singlePool(
              ErrorStrategy.skipOnError(),
              Duration.ZERO,
              r -> {
                processed.add(r.offset());
                Thread.currentThread().interrupt(); // the usual "restore the flag" idiom
              },
              ProcessingLifecycleHook.noOp());
      pool.start();
      registerAndDispatch(0);
      assertTrue(committedTo(1));
      registerAndDispatch(1);
      registerAndDispatch(2);

      assertTrue(committedTo(3), "the only worker must keep running after a leaked interrupt");
      assertEquals(List.of(0L, 1L, 2L), processed);
    }

    @Test
    void interruptedFailureWhileRunning_isHandledNotLeftInProgress() throws Exception {
      AtomicInteger attempts = new AtomicInteger();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder().maxRetries(1).retryBackoff(Duration.ZERO).build();
      pool =
          singlePool(
              strategy,
              Duration.ZERO,
              r -> {
                attempts.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new InterruptedException("handler's own I/O was interrupted");
              },
              ProcessingLifecycleHook.noOp());
      pool.start();
      registerAndDispatch(0);

      // Retried, then skipped and acked, instead of being left IN_PROGRESS as a "shutdown".
      assertTrue(committedTo(1));
      assertEquals(2, attempts.get());
    }
  }

  @Nested
  class ProcessingTimeout {

    @Test
    void hungHandler_isInterruptedAndPartitionMovesOn() throws Exception {
      List<Long> processed = new CopyOnWriteArrayList<>();
      AtomicReference<Exception> seen = new AtomicReference<>();
      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .fallback(Fallback.SKIP)
              .onFinalFailure((r, e) -> seen.set(e))
              .build();
      pool =
          singlePool(
              strategy,
              Duration.ofMillis(100),
              r -> {
                if (r.offset() == 0) Thread.sleep(60_000); // hangs until interrupted
                processed.add(r.offset());
              },
              ProcessingLifecycleHook.noOp());
      pool.start();
      registerAndDispatch(0);
      registerAndDispatch(1);

      assertTrue(committedTo(2), "the hung record must not hold back the partition");
      assertEquals(List.of(1L), processed);
      assertInstanceOf(TimeoutException.class, seen.get());
    }

    @Test
    void timeoutDuringShutdown_leavesRecordForRedelivery() {
      RetryExecutor<String, String> executor =
          new RetryExecutor<>(
              ErrorStrategy.skipOnError(),
              NoOpMetricsCollector.INSTANCE,
              HandlerAttemptObserver.NONE,
              Duration.ofMillis(20));
      try {
        Exception result =
            executor.executeWithRetries(
                attempt -> {
                  long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
                  while (System.nanoTime() < until) Thread.onSpinWait(); // ignores the interrupt
                  throw new IllegalStateException("failed after the deadline");
                },
                TP0,
                "slow",
                () -> true); // the pipeline is stopping meanwhile
        assertInstanceOf(InterruptedException.class, result, "a shutdown, not a timeout to skip");
        assertTrue(Thread.interrupted(), "interrupt flag kept for the caller");
      } finally {
        executor.close();
      }
    }

    @Test
    void attemptAfterClose_runsWithoutTimer() {
      RetryExecutor<String, String> executor =
          new RetryExecutor<>(
              ErrorStrategy.skipOnError(),
              NoOpMetricsCollector.INSTANCE,
              HandlerAttemptObserver.NONE,
              Duration.ofMillis(20));
      executor.close();
      assertNull(executor.executeWithRetries(attempt -> {}, TP0, "late", () -> true));
    }

    @Test
    void fastAttempts_neverSeeAStrayTimeoutInterrupt() throws Exception {
      RetryExecutor<String, String> executor =
          new RetryExecutor<>(
              ErrorStrategy.skipOnError(),
              NoOpMetricsCollector.INSTANCE,
              HandlerAttemptObserver.NONE,
              Duration.ofMillis(1));
      try {
        for (int i = 0; i < 200; i++) {
          Exception result =
              executor.executeWithRetries(
                  attempt -> {
                    long until = System.nanoTime() + 900_000; // close to the 1 ms deadline
                    while (System.nanoTime() < until) Thread.onSpinWait();
                  },
                  TP0,
                  "spin",
                  () -> false);
          assertTrue(result == null || result instanceof TimeoutException);
          assertFalse(Thread.currentThread().isInterrupted(), "iteration " + i);
        }
      } finally {
        executor.close();
      }
    }
  }

  @Nested
  class DispatchFailure {

    @Test
    void uninitializedPartition_isSentBackWithoutDroppingOthers() {
      // TP1 is assigned but has no offset window (its initialization failed).
      dispatcher.addPartition(TP1);
      pool =
          singlePool(
              ErrorStrategy.skipOnError(), Duration.ZERO, r -> {}, ProcessingLifecycleHook.noOp());

      Map<TopicPartition, List<ConsumerRecord<String, String>>> polled = new LinkedHashMap<>();
      polled.put(TP1, List.of(new ConsumerRecord<>("test", 1, 7L, "k", "v")));
      polled.put(
          TP0,
          List.of(
              new ConsumerRecord<>("test", 0, 0L, "k", "v"),
              new ConsumerRecord<>("test", 0, 1L, "k", "v")));
      ConsumerRecords<String, String> records = new ConsumerRecords<>(polled, Map.of());

      Map<TopicPartition, Long> rejected = pool.dispatch(records);

      assertEquals(Map.of(TP1, 7L), rejected, "the failing partition is rewound");
      assertEquals(2, dispatcher.queuedRecords(TP0), "later partitions are still dispatched");
      assertEquals(2, tracker.pendingCount(TP0));
    }
  }

  @Nested
  class BatchPartitionLimit {

    @Test
    void unorderedBatches_respectThePerPartitionLimit() throws Exception {
      CountDownLatch release = new CountDownLatch(1);
      BatchWorkerPool<String, String> batchPool =
          new BatchWorkerPool<>(
              4,
              ThreadMode.PLATFORM,
              (tp, batch) -> release.await(),
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              100,
              NoOpMetricsCollector.INSTANCE,
              ProcessingOrder.UNORDERED);
      batchPool.partitionLimit(() -> 1);
      pool = batchPool;
      pool.start();

      assertTrue(pool.dispatch(batch(0, 1)).isEmpty());
      assertTrue(pool.holdsPartition(TP0), "a partition at its limit stays paused");
      assertEquals(Map.of(TP0, 2L), pool.dispatch(batch(2, 3)), "second batch is sent back");

      release.countDown();
      assertTrue(await(() -> !pool.holdsPartition(TP0), 5000));
      assertTrue(pool.dispatch(batch(2, 3)).isEmpty());
      assertTrue(committedTo(4));
    }

    private ConsumerRecords<String, String> batch(long from, long to) {
      List<ConsumerRecord<String, String>> list = new java.util.ArrayList<>();
      for (long o = from; o <= to; o++) list.add(new ConsumerRecord<>("test", 0, o, "k", "v"));
      ConsumerRecords<String, String> records = new ConsumerRecords<>(Map.of(TP0, list), Map.of());
      return records;
    }
  }
}
