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
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
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
    void fullTaskQueue_rejectsBatchWithoutBlockingTheCaller() throws Exception {
      TopicPartition tp1 = new TopicPartition("test", 1);
      TopicPartition tp2 = new TopicPartition("test", 2);
      tracker.initPartition(tp1, 0);
      tracker.initPartition(tp2, 0);

      CountDownLatch blockLatch = new CountDownLatch(1);
      CountDownLatch taskStarted = new CountDownLatch(1);
      AtomicInteger completedBatches = new AtomicInteger();

      // concurrency=1 + taskQueueCapacity=1: one thread occupied + one slot in queue.
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
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              1,
              NoOpMetricsCollector.INSTANCE);
      pool.start();

      // First dispatch (TP0) occupies the single thread
      assertTrue(pool.dispatch(buildRecords(TP0, 0, 0)).isEmpty());
      assertTrue(taskStarted.await(3, TimeUnit.SECONDS));
      // Second dispatch (tp1) fills the queue (capacity=1)
      assertTrue(pool.dispatch(buildRecords(tp1, 0, 0)).isEmpty());
      assertFalse(pool.canResume(tp2));

      // Third dispatch (tp2) finds no room: returned for a seek-back, nothing registered.
      long start = System.nanoTime();
      Map<TopicPartition, Long> rejected = pool.dispatch(buildRecords(tp2, 5, 6));
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1), "must not block");
      assertEquals(Map.of(tp2, 5L), rejected);
      assertEquals(0, tracker.windowSize(tp2));

      blockLatch.countDown();
      long deadline = System.currentTimeMillis() + 5_000;
      while (completedBatches.get() < 2 && System.currentTimeMillis() < deadline) Thread.sleep(10);
      assertTrue(pool.canResume(tp2), "queue drained, partition may resume");
      pool.stop(5000);
    }
  }

  @Nested
  class SkipCompleted {

    @Test
    void recordsFinishedByThePreviousOwner_areLeftOutOfTheBatch() throws Exception {
      // Previous owner: 0 stuck, 1, 2 and 4 finished.
      UnorderedOffsetTracker previous = new UnorderedOffsetTracker(100, true);
      TopicPartition tp = new TopicPartition("test", 9);
      previous.initPartition(tp, 0, null);
      previous.registerBatch(tp, new long[] {0, 1, 2, 3, 4, 5, 6, 7});
      previous.markBatchInProgress(tp, new long[] {1, 2, 4, 7});
      previous.ackBatch(tp, new long[] {1, 2, 4, 7});
      String metadata = previous.getAllCommitPoints().get(tp).metadata();

      tracker = new UnorderedOffsetTracker(100, true);
      tracker.initPartition(tp, 0, metadata);
      List<Long> handled = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
      CountDownLatch done = new CountDownLatch(1);
      BatchWorkerPool<String, String> pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (t, records) -> {
                records.forEach(r -> handled.add(r.offset()));
                done.countDown();
              },
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              10,
              NoOpMetricsCollector.INSTANCE);
      pool.start();

      assertTrue(pool.dispatch(buildRecords(tp, 0, 0)).isEmpty());
      assertTrue(done.await(5, TimeUnit.SECONDS));
      // A batch made only of finished records is not submitted at all.
      assertTrue(pool.dispatch(buildRecords(tp, 1, 2)).isEmpty());
      // A mixed batch keeps only the unfinished ones.
      long deadline = System.currentTimeMillis() + 5_000;
      while (tracker.getCommittableOffset(tp).orElse(0) < 3
          && System.currentTimeMillis() < deadline) Thread.sleep(10);
      // Mixed batches keep only the unfinished records (here the finished one is last).
      // Finished record in the middle (4), then at the end (7).
      assertTrue(pool.dispatch(buildRecords(tp, 3, 5)).isEmpty());
      while (tracker.getCommittableOffset(tp).orElse(0) < 6
          && System.currentTimeMillis() < deadline) Thread.sleep(10);
      assertTrue(pool.dispatch(buildRecords(tp, 6, 7)).isEmpty());
      while (tracker.getCommittableOffset(tp).orElse(0) < 8
          && System.currentTimeMillis() < deadline) Thread.sleep(10);
      assertEquals(List.of(0L, 3L, 5L, 6L), List.copyOf(handled));
      assertEquals(8, tracker.getCommittableOffset(tp).getAsLong());
      assertEquals(0, counter.records());
      pool.stop(1000);
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
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              100,
              NoOpMetricsCollector.INSTANCE);
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
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              100,
              NoOpMetricsCollector.INSTANCE);
      pool.start();

      pool.dispatch(buildRecords(TP0, 0, 4));

      assertTrue(done.await(3, TimeUnit.SECONDS));
      assertEquals(5, processed.size());
      // Wait for counter.completed() which runs after handler returns
      Thread.sleep(50);
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
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              100,
              NoOpMetricsCollector.INSTANCE);
      pool.start();

      pool.dispatch(buildRecords(TP0, 0, 2));

      assertTrue(
          awaitCondition(() -> counter.records() == 0, Duration.ofSeconds(3)),
          "Counter should be decremented after skip");
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
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              100,
              NoOpMetricsCollector.INSTANCE);
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
              .build();

      pool =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> {
                throw new RuntimeException("always fails");
              },
              new RetryExecutor<>(strategy, NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              100,
              NoOpMetricsCollector.INSTANCE);
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

  @Nested
  class FullWindow {

    @Test
    void dispatch_rejectsWholeBatchPerFullPartition_andReportsBatchStart() {
      TopicPartition tp1 = new TopicPartition("test", 1);
      TopicPartition tp2 = new TopicPartition("test", 2);
      tracker = new UnorderedOffsetTracker(3);
      tracker.initPartition(TP0, 0);
      tracker.initPartition(tp1, 0);
      tracker.initPartition(tp2, 0);
      AtomicInteger handled = new AtomicInteger();

      BatchWorkerPool<String, String> pool =
          new BatchWorkerPool<>(
              1,
              ThreadMode.PLATFORM,
              (tp, records) -> handled.addAndGet(records.size()),
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              0,
              NoOpMetricsCollector.INSTANCE);

      Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
      map.put(TP0, buildRecords(TP0, 0, 4).records(TP0)); // 5 records > window of 3
      map.put(tp1, buildRecords(tp1, 7, 10).records(tp1)); // 4 records > window of 3
      map.put(tp2, buildRecords(tp2, 0, 1).records(tp2)); // fits
      Map<TopicPartition, Long> rejected = pool.dispatch(new ConsumerRecords<>(map, Map.of()));

      assertEquals(Map.of(TP0, 0L, tp1, 7L), rejected);
      assertEquals(0, tracker.windowSize(TP0));
      assertEquals(0, tracker.windowSize(tp1));
      pool.stop(5_000); // waits for tp2's batch, which fit
      assertEquals(2, handled.get());
      assertEquals(0, counter.records());
    }
  }

  @Nested
  class VirtualConcurrency {

    @Test
    void virtualThreads_runAtMostConcurrencyHandlersAtOnce() throws Exception {
      AtomicInteger running = new AtomicInteger();
      AtomicInteger peak = new AtomicInteger();
      AtomicInteger done = new AtomicInteger();
      int partitions = 12;
      for (int p = 1; p < partitions; p++) tracker.initPartition(new TopicPartition("test", p), 0);

      BatchWorkerPool<String, String> pool =
          new BatchWorkerPool<>(
              3,
              ThreadMode.VIRTUAL,
              (tp, records) -> {
                peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                Thread.sleep(30);
                running.decrementAndGet();
                done.incrementAndGet();
              },
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              0,
              NoOpMetricsCollector.INSTANCE);
      pool.start();
      for (int p = 0; p < partitions; p++) {
        TopicPartition tp = new TopicPartition("test", p);
        pool.dispatch(buildRecords(tp, 0, 1));
      }

      long deadline = System.currentTimeMillis() + 5_000;
      while (done.get() < partitions && System.currentTimeMillis() < deadline) Thread.sleep(10);
      pool.stop(5_000);

      assertEquals(partitions, done.get());
      assertEquals(3, peak.get(), "concurrency=3 must cap concurrent batch handlers");
      assertEquals(0, counter.records());
    }

    @Test
    void shutdownWhileWaitingForPermit_leavesBatchUnackedAndReleasesCount() throws Exception {
      TopicPartition tp1 = new TopicPartition("test", 1);
      tracker.initPartition(tp1, 0);
      CountDownLatch firstStarted = new CountDownLatch(1);
      CountDownLatch block = new CountDownLatch(1);
      AtomicInteger handled = new AtomicInteger();

      BatchWorkerPool<String, String> pool =
          new BatchWorkerPool<>(
              1,
              ThreadMode.VIRTUAL,
              (tp, records) -> {
                firstStarted.countDown();
                handled.incrementAndGet();
                block.await(10, TimeUnit.SECONDS);
              },
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              0,
              NoOpMetricsCollector.INSTANCE);
      pool.start();
      pool.dispatch(buildRecords(TP0, 0, 1));
      assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
      pool.dispatch(buildRecords(tp1, 0, 1)); // waits for the single permit

      pool.stop(100); // times out, interrupts both

      assertEquals(1, handled.get(), "the waiting batch must never reach the handler");
      long deadline = System.currentTimeMillis() + 2_000;
      while (counter.records() != 0 && System.currentTimeMillis() < deadline) Thread.sleep(10);
      assertEquals(0, counter.records());
      assertEquals(2, tracker.pendingCount(tp1), "waiting batch stays unacked for redelivery");
    }
  }

  @Nested
  class OrderedAndStopping {

    private BatchWorkerPool<String, String> pool(
        ThreadMode mode,
        io.github.openlogiclab.kafkapipeline.handler.BatchRecordHandler<String, String> handler,
        io.github.openlogiclab.kafkapipeline.ProcessingOrder order) {
      return new BatchWorkerPool<>(
          1,
          mode,
          handler,
          new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
          tracker,
          counter,
          10,
          NoOpMetricsCollector.INSTANCE,
          order);
    }

    @Test
    void secondBatchOfBusyPartitions_isSentBackForEachOfThem() throws Exception {
      TopicPartition tp1 = new TopicPartition("test", 1);
      tracker.initPartition(tp1, 0);
      CountDownLatch release = new CountDownLatch(1);
      BatchWorkerPool<String, String> p =
          new BatchWorkerPool<>(
              2,
              ThreadMode.PLATFORM,
              (tp, records) -> release.await(5, TimeUnit.SECONDS),
              new RetryExecutor<>(ErrorStrategy.skipOnError(), NoOpMetricsCollector.INSTANCE),
              tracker,
              counter,
              10,
              NoOpMetricsCollector.INSTANCE,
              io.github.openlogiclab.kafkapipeline.ProcessingOrder.PARTITION);
      p.start();
      Map<TopicPartition, List<ConsumerRecord<String, String>>> first = new HashMap<>();
      first.put(TP0, buildRecords(TP0, 0, 1).records(TP0));
      first.put(tp1, buildRecords(tp1, 0, 1).records(tp1));
      assertTrue(p.dispatch(new ConsumerRecords<>(first, Map.of())).isEmpty());

      Map<TopicPartition, List<ConsumerRecord<String, String>>> second = new HashMap<>();
      second.put(TP0, buildRecords(TP0, 2, 3).records(TP0));
      second.put(tp1, buildRecords(tp1, 2, 3).records(tp1));
      assertEquals(Map.of(TP0, 2L, tp1, 2L), p.dispatch(new ConsumerRecords<>(second, Map.of())));
      release.countDown();
      p.stop(2000);
    }

    @Test
    void batchWaitingForAWorkerWhenStopIsCalled_neverRuns() throws Exception {
      TopicPartition tp1 = new TopicPartition("test", 1);
      tracker.initPartition(tp1, 0);
      CountDownLatch firstRunning = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      AtomicInteger handled = new AtomicInteger();
      BatchWorkerPool<String, String> p =
          pool(
              ThreadMode.VIRTUAL,
              (tp, records) -> {
                handled.incrementAndGet();
                firstRunning.countDown();
                // Not interruptible: models a handler that finishes on its own after stop().
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (release.getCount() > 0 && System.nanoTime() < end) Thread.onSpinWait();
              },
              io.github.openlogiclab.kafkapipeline.ProcessingOrder.UNORDERED);
      p.start();
      p.dispatch(buildRecords(TP0, 0, 0));
      assertTrue(firstRunning.await(5, TimeUnit.SECONDS));
      p.dispatch(buildRecords(tp1, 0, 0)); // waits for the only permit

      Thread stopper = new Thread(() -> p.stop(5_000));
      stopper.start();
      Thread.sleep(100); // stop() has cleared the running flag and waits for termination
      release.countDown(); // the first batch finishes and hands over its permit
      stopper.join(10_000);

      assertEquals(1, handled.get(), "the waiting batch must not run after stop()");
      assertEquals(1, tracker.pendingCount(tp1), "left for redelivery");
    }
  }
}
