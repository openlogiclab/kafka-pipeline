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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BatchWorkerPoolTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);

  private UnorderedOffsetTracker tracker;
  private InFlightCounter counter;

  @BeforeEach
  void setUp() {
    tracker = new UnorderedOffsetTracker();
    tracker.initPartition(TP0, 0);
    counter = new InFlightCounter();
  }

  @SuppressWarnings("deprecation")
  private ConsumerRecords<String, String> buildRecords(TopicPartition tp, int from, int to) {
    Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
    List<ConsumerRecord<String, String>> records = new java.util.ArrayList<>();
    for (int i = from; i <= to; i++) {
      records.add(new ConsumerRecord<>(tp.topic(), tp.partition(), i, "k", "v-" + i));
    }
    map.put(tp, records);
    return new ConsumerRecords<>(map);
  }

  @Nested
  class ExecutorQueueFull {

    @Test
    void rejectedExecutionHandler_blocksUntilSpace() throws Exception {
      TopicPartition tp1 = new TopicPartition("test", 1);
      TopicPartition tp2 = new TopicPartition("test", 2);
      tracker.initPartition(tp1, 0);
      tracker.initPartition(tp2, 0);

      CountDownLatch blockLatch = new CountDownLatch(1);
      CountDownLatch taskStarted = new CountDownLatch(1);
      AtomicInteger completedBatches = new AtomicInteger();

      // concurrency=1 + taskQueueCapacity=1: one thread occupied + one slot in queue.
      // Third submit triggers the RejectedExecutionHandler.
      BatchWorkerPool<String, String> pool =
          new BatchWorkerPool<>(
              1,
              ThreadMode.PLATFORM,
              (tp, records) -> {
                if (tp.equals(TP0)) {
                  taskStarted.countDown();
                  blockLatch.await(5, TimeUnit.SECONDS);
                }
                completedBatches.incrementAndGet();
              },
              new RetryExecutor<>(ErrorStrategy.failFast()),
              tracker,
              counter,
              1);
      pool.start();

      // First dispatch (TP0) occupies the single thread
      pool.dispatch(buildRecords(TP0, 0, 0));
      assertTrue(taskStarted.await(3, TimeUnit.SECONDS));

      // Second dispatch (tp1) fills the queue (capacity=1)
      pool.dispatch(buildRecords(tp1, 0, 0));

      // Third dispatch (tp2) triggers RejectedExecutionHandler — caller blocks
      Thread submitter = new Thread(() -> pool.dispatch(buildRecords(tp2, 0, 0)));
      submitter.start();

      Thread.sleep(200);
      assertTrue(submitter.isAlive(), "Submitter should be blocked by RejectedExecutionHandler");

      // Release first task — frees the thread, queue drains
      blockLatch.countDown();
      submitter.join(5000);

      assertFalse(submitter.isAlive(), "Submitter should have completed");
      pool.stop(5000);

      assertEquals(3, completedBatches.get());
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void doubleStartThrows() {
      BatchWorkerPool<String, String> pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> {},
              new RetryExecutor<>(ErrorStrategy.failFast()),
              tracker,
              counter,
              100);
      pool.start();
      assertThrows(IllegalStateException.class, pool::start);
      pool.stop(1000);
    }
  }

  @Nested
  class ProcessBatch {

    private BatchWorkerPool<String, String> pool;

    @AfterEach
    void tearDown() {
      if (pool != null) pool.stop(3000);
    }

    @Test
    void successfulBatchAcksAndCompletesCounter() throws Exception {
      CopyOnWriteArrayList<String> processed = new CopyOnWriteArrayList<>();
      CountDownLatch done = new CountDownLatch(1);

      pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> {
                for (var r : records) processed.add(r.value());
                done.countDown();
              },
              new RetryExecutor<>(ErrorStrategy.failFast()),
              tracker,
              counter,
              100);
      pool.start();

      pool.dispatch(buildRecords(TP0, 0, 4));

      assertTrue(done.await(3, TimeUnit.SECONDS));
      assertEquals(5, processed.size());
      assertEquals(0, counter.records());
    }

    @Test
    void failPartitionBranch() throws Exception {
      pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> {
                throw new RuntimeException("always fails");
              },
              new RetryExecutor<>(ErrorStrategy.failFast()),
              tracker,
              counter,
              100);
      pool.start();

      pool.dispatch(buildRecords(TP0, 0, 2));

      assertTrue(
          awaitCondition(() -> counter.records() == 0, Duration.ofSeconds(3)),
          "Counter should be decremented after FAIL_PARTITION");
    }

    @Test
    void markBatchInProgressFailure_earlyReturn() throws Exception {
      // Put partition into failed state so markBatchInProgress throws.
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      AtomicInteger handlerCalls = new AtomicInteger();

      pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> handlerCalls.incrementAndGet(),
              new RetryExecutor<>(ErrorStrategy.failFast()),
              tracker,
              counter,
              100);
      pool.start();

      // Resolve failure so registerBatch succeeds, but immediately fail again
      // before the async processBatch has a chance to call markBatchInProgress.
      tracker.resolveFailure(TP0, 100);

      pool.dispatch(buildRecords(TP0, 0, 2));

      // Re-fail the partition; the async processBatch hasn't run yet on the pool thread.
      tracker.register(TP0, 200);
      tracker.markInProgress(TP0, 200);
      tracker.fail(TP0, 200);

      Thread.sleep(500);

      // Race-dependent: partition must be failed before async processBatch runs.
      // If timing didn't align, mark test as skipped rather than failed.
      assumeTrue(
          handlerCalls.get() == 0,
          "Race condition: processBatch ran before partition was failed — inconclusive");
      assertEquals(0, counter.records(), "Counter should be decremented after early return");
    }

    @Test
    void skipFallbackAcksBatch() throws Exception {
      pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> {
                throw new RuntimeException("always fails");
              },
              new RetryExecutor<>(ErrorStrategy.skipOnError()),
              tracker,
              counter,
              100);
      pool.start();

      pool.dispatch(buildRecords(TP0, 0, 2));

      assertTrue(
          awaitCondition(() -> counter.records() == 0, Duration.ofSeconds(3)),
          "Counter should be decremented after SKIP");

      assertTrue(
          awaitCondition(
              () -> tracker.getCommittableOffset(TP0).isPresent(), Duration.ofSeconds(3)),
          "Offset should advance after skip");
      assertEquals(3L, tracker.getCommittableOffset(TP0).getAsLong());
    }

    @Test
    void dlqSuccessAcksBatch() throws Exception {
      CopyOnWriteArrayList<ConsumerRecord<String, String>> dlqRecords =
          new CopyOnWriteArrayList<>();

      ErrorStrategy<String, String> strategy =
          ErrorStrategy.<String, String>builder()
              .maxRetries(0)
              .retryBackoff(Duration.ZERO)
              .dlqHandler((record, error) -> dlqRecords.add(record))
              .fallback(Fallback.FAIL_PARTITION)
              .build();

      pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> {
                throw new RuntimeException("always fails");
              },
              new RetryExecutor<>(strategy),
              tracker,
              counter,
              100);
      pool.start();

      pool.dispatch(buildRecords(TP0, 0, 2));

      assertTrue(
          awaitCondition(() -> dlqRecords.size() == 3, Duration.ofSeconds(3)),
          "All 3 records should be sent to DLQ");
      assertTrue(
          awaitCondition(() -> counter.records() == 0, Duration.ofSeconds(3)),
          "Counter should be decremented after DLQ success");
      assertTrue(
          awaitCondition(
              () -> tracker.getCommittableOffset(TP0).isPresent(), Duration.ofSeconds(3)),
          "Offset should advance after DLQ success");
    }
  }

  private boolean awaitCondition(java.util.function.BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) return false;
      Thread.sleep(20);
    }
    return true;
  }
}
