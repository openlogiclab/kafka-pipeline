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

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class WorkerPoolTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);

  private UnorderedOffsetTracker tracker;
  private RecordDispatcher<String, String> dispatcher;
  private InFlightCounter counter;

  @BeforeEach
  void setUp() {
    tracker = new UnorderedOffsetTracker();
    tracker.initPartition(TP0, 0);
    dispatcher = new RecordDispatcher<>(100);
    dispatcher.addPartition(TP0);
    counter = new InFlightCounter();
  }

  private void registerAndDispatch(long offset) {
    tracker.register(TP0, offset);
    counter.registered(1, 0);
    dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, offset, "k", "v-" + offset));
  }

  private SingleRecordWorkerPool<String, String> pool(
      int concurrency,
      ErrorStrategy<String, String> errorStrategy,
      RecordHandler<String, String> handler,
      ProcessingLifecycleHook<String, String> hook) {
    return new SingleRecordWorkerPool<>(
        concurrency,
        ThreadMode.PLATFORM,
        handler,
        hook,
        new RetryExecutor<>(errorStrategy),
        tracker,
        dispatcher,
        counter);
  }

  private WorkerPool<String, String> pool(RecordHandler<String, String> handler) {
    return pool(1, ErrorStrategy.failFast(), handler, ProcessingLifecycleHook.noOp());
  }

  @Nested
  class Construction {

    @Test
    void rejectsZeroConcurrency() {
      assertThrows(
          IllegalArgumentException.class,
          () -> pool(0, ErrorStrategy.failFast(), r -> {}, ProcessingLifecycleHook.noOp()));
    }

    @Test
    void rejectsNegativeConcurrency() {
      assertThrows(
          IllegalArgumentException.class,
          () -> pool(-1, ErrorStrategy.failFast(), r -> {}, ProcessingLifecycleHook.noOp()));
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void isRunning_falseBeforeStart() {
      WorkerPool<String, String> wp = pool(r -> {});
      assertFalse(wp.isRunning());
    }

    @Test
    void isRunning_trueAfterStart() {
      WorkerPool<String, String> wp = pool(r -> {});
      wp.start();
      assertTrue(wp.isRunning());
      wp.stop(1000);
    }

    @Test
    void isRunning_falseAfterStop() {
      WorkerPool<String, String> wp = pool(r -> {});
      wp.start();
      wp.stop(1000);
      assertFalse(wp.isRunning());
    }

    @Test
    void doubleStart_throws() {
      WorkerPool<String, String> wp = pool(r -> {});
      wp.start();
      assertThrows(IllegalStateException.class, wp::start);
      wp.stop(1000);
    }

    @Test
    void stopWithTimeout_forcesShutdown() throws Exception {
      CountDownLatch stuck = new CountDownLatch(1);
      WorkerPool<String, String> wp =
          pool(
              record -> {
                stuck.countDown();
                Thread.sleep(60_000);
              });

      registerAndDispatch(0);
      wp.start();
      assertTrue(stuck.await(2, TimeUnit.SECONDS));
      wp.stop(100);
      assertFalse(wp.isRunning());
    }
  }

  @Nested
  class MarkInProgressFailure {

    @Test
    void markInProgressThrows_recordSkipped() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      WorkerPool<String, String> wp = pool(record -> processed.add(record.value()));

      dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, 99L, "k", "orphan"));

      wp.start();
      Thread.sleep(300);
      wp.stop(1000);

      assertTrue(processed.isEmpty());
    }
  }

  @Nested
  class BeforeProcessHookThrows {

    @Test
    void hookException_proceedsWithHandler() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      ProcessingLifecycleHook<String, String> throwingHook =
          new ProcessingLifecycleHook<>() {
            @Override
            public boolean beforeProcess(
                ConsumerRecord<String, String> record, ProcessingContext ctx) {
              throw new RuntimeException("hook boom");
            }
          };

      WorkerPool<String, String> wp =
          pool(1, ErrorStrategy.failFast(), record -> processed.add(record.value()), throwingHook);

      registerAndDispatch(0);
      wp.start();
      Thread.sleep(300);
      wp.stop(1000);

      assertEquals(1, processed.size());
      assertEquals("v-0", processed.getFirst());
    }
  }

  @Nested
  class DlqFailsThenFallback {

    @Test
    void dlqFails_fallbackSkip() throws Exception {
      AtomicInteger dlqAttempts = new AtomicInteger();

      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(0)
              .retryBackoff(Duration.ZERO)
              .dlqHandler(
                  (record, error) -> {
                    dlqAttempts.incrementAndGet();
                    throw new RuntimeException("DLQ send failed");
                  })
              .fallback(Fallback.SKIP)
              .build();

      WorkerPool<String, String> wp =
          pool(
              1,
              strategy,
              record -> {
                throw new RuntimeException("always fails");
              },
              ProcessingLifecycleHook.noOp());

      registerAndDispatch(0);
      wp.start();
      Thread.sleep(300);
      wp.stop(1000);

      assertEquals(1, dlqAttempts.get());
    }

    @Test
    void dlqFails_fallbackFailPartition() throws Exception {
      AtomicInteger dlqAttempts = new AtomicInteger();

      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(0)
              .retryBackoff(Duration.ZERO)
              .dlqHandler(
                  (record, error) -> {
                    dlqAttempts.incrementAndGet();
                    throw new RuntimeException("DLQ send failed");
                  })
              .fallback(Fallback.FAIL_PARTITION)
              .build();

      WorkerPool<String, String> wp =
          pool(
              1,
              strategy,
              record -> {
                throw new RuntimeException("always fails");
              },
              ProcessingLifecycleHook.noOp());

      registerAndDispatch(0);
      wp.start();
      Thread.sleep(300);
      wp.stop(1000);

      assertEquals(1, dlqAttempts.get());
    }
  }

  @Nested
  class NoDlqFailPartition {

    @Test
    void noDlq_failPartition() throws Exception {
      ErrorStrategy<String, String> strategy = ErrorStrategy.failFast();

      WorkerPool<String, String> wp =
          pool(
              1,
              strategy,
              record -> {
                throw new RuntimeException("boom");
              },
              ProcessingLifecycleHook.noOp());

      registerAndDispatch(0);
      wp.start();
      Thread.sleep(300);
      wp.stop(1000);
    }

    @Test
    void noDlq_skip() throws Exception {
      ErrorStrategy<String, String> strategy = ErrorStrategy.skipOnError();
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      WorkerPool<String, String> wp =
          pool(
              1,
              strategy,
              record -> {
                if (record.offset() == 0) throw new RuntimeException("skip me");
                processed.add(record.value());
              },
              ProcessingLifecycleHook.noOp());

      registerAndDispatch(0);
      registerAndDispatch(1);
      wp.start();
      Thread.sleep(500);
      wp.stop(1000);

      assertEquals(1, processed.size());
      assertEquals("v-1", processed.getFirst());
    }
  }

  @Nested
  class StopBehavior {

    @Test
    void stopInterrupted_callsShutdownNow() throws Exception {
      WorkerPool<String, String> wp = pool(r -> {});
      wp.start();

      Thread stopper =
          new Thread(
              () -> {
                Thread.currentThread().interrupt();
                wp.stop(5000);
              });
      stopper.start();
      stopper.join(3000);

      assertFalse(wp.isRunning());
    }
  }

  @Nested
  class FullPipeline {

    @Test
    void processMultipleRecords() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      WorkerPool<String, String> wp = pool(record -> processed.add(record.value()));

      for (int i = 0; i < 10; i++) {
        registerAndDispatch(i);
      }

      wp.start();
      Thread.sleep(500);
      wp.stop(2000);

      assertEquals(10, processed.size());
    }

    @Test
    void retryThenSuccess() throws Exception {
      AtomicInteger attempts = new AtomicInteger();
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();

      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(2)
              .retryBackoff(Duration.ofMillis(10))
              .fallback(Fallback.FAIL_PARTITION)
              .build();

      WorkerPool<String, String> wp =
          pool(
              1,
              strategy,
              record -> {
                if (attempts.incrementAndGet() <= 2) throw new RuntimeException("transient");
                processed.add(record.value());
              },
              ProcessingLifecycleHook.noOp());

      registerAndDispatch(0);
      wp.start();
      Thread.sleep(500);
      wp.stop(1000);

      assertEquals(1, processed.size());
      assertEquals(3, attempts.get());
    }
  }
}
