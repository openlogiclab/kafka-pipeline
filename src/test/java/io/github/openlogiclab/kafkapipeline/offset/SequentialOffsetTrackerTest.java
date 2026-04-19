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
package io.github.openlogiclab.kafkapipeline.offset;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SequentialOffsetTrackerTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);
  private static final TopicPartition TP2 = new TopicPartition("test", 2);
  private static final TopicPartition TP_UNKNOWN = new TopicPartition("test", 99);

  private SequentialOffsetTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new SequentialOffsetTracker();
  }

  // ── Lifecycle ────────────────────────────────────────────────

  @Nested
  class Lifecycle {

    @Test
    void initPartition_createsTrackingState() {
      tracker.initPartition(TP0, 100);
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));
      assertEquals(0, tracker.lag(TP0));
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
    }

    @Test
    void initPartition_duplicateThrows() {
      tracker.initPartition(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.initPartition(TP0, 200));
    }

    @Test
    void clearPartition_removesState() {
      tracker.initPartition(TP0, 100);
      tracker.clearPartition(TP0);
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
    }

    @Test
    void clearPartition_nonExistentIsNoOp() {
      assertDoesNotThrow(() -> tracker.clearPartition(TP_UNKNOWN));
    }

    @Test
    void clearPartition_withPendingRecords_logsWarning() {
      tracker.initPartition(TP0, 100);
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);

      assertDoesNotThrow(() -> tracker.clearPartition(TP0));
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
    }

    @Test
    void clearPartition_withInProgressRecord_logsWarning() {
      tracker.initPartition(TP0, 100);
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);

      assertDoesNotThrow(() -> tracker.clearPartition(TP0));
      assertEquals(0, tracker.pendingCount(TP0));
    }

    @Test
    void clearPartition_thenReinitSucceeds() {
      tracker.initPartition(TP0, 100);
      tracker.register(TP0, 100);
      processOneRecord(TP0, 100);

      tracker.clearPartition(TP0);
      tracker.initPartition(TP0, 500);

      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
      assertEquals(0, tracker.pendingCount(TP0));
    }
  }

  // ── Happy Path ───────────────────────────────────────────────

  @Nested
  class HappyPath {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void singleRecord_fullLifecycle() {
      tracker.register(TP0, 100);
      assertEquals(1, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));

      tracker.markInProgress(TP0, 100);
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(1, tracker.inProgressCount(TP0));

      tracker.ack(TP0, 100);
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }

    @Test
    void multipleRecords_processedSequentially() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      tracker.register(TP0, 102);
      assertEquals(3, tracker.pendingCount(TP0));

      processOneRecord(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
      assertEquals(2, tracker.pendingCount(TP0));

      processOneRecord(TP0, 101);
      assertEquals(OptionalLong.of(102), tracker.getCommittableOffset(TP0));

      processOneRecord(TP0, 102);
      assertEquals(OptionalLong.of(103), tracker.getCommittableOffset(TP0));
      assertEquals(0, tracker.pendingCount(TP0));
    }

    @Test
    void committableOffset_followsKafkaSemantics_offsetPlusOne() {
      tracker.register(TP0, 100);
      processOneRecord(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }

    @Test
    void getCommittableOffset_emptyBeforeAnyProgress() {
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));

      tracker.register(TP0, 100);
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));

      tracker.markInProgress(TP0, 100);
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
    }

    @Test
    void getCommittableOffset_isIdempotent() {
      tracker.register(TP0, 100);
      processOneRecord(TP0, 100);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }

    @Test
    void nonContiguousOffsets_compactedTopic() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 105);

      processOneRecord(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));

      processOneRecord(TP0, 105);
      assertEquals(OptionalLong.of(106), tracker.getCommittableOffset(TP0));
    }
  }

  // ── Batch Registration ───────────────────────────────────────

  @Nested
  class BatchRegistration {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void registerBatch_registersCorrectCount() {
      tracker.registerBatch(TP0, 100, 104);
      assertEquals(5, tracker.pendingCount(TP0));
      assertEquals(5, tracker.lag(TP0));
    }

    @Test
    void registerBatch_singleOffset() {
      tracker.registerBatch(TP0, 100, 100);
      assertEquals(1, tracker.pendingCount(TP0));
    }

    @Test
    void registerBatch_invalidRangeThrows() {
      assertThrows(IllegalArgumentException.class, () -> tracker.registerBatch(TP0, 104, 100));
    }

    @Test
    void registerBatch_overlappingThrows() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.registerBatch(TP0, 99, 102));
    }

    @Test
    void registerBatch_followedByIndividualRegister() {
      tracker.registerBatch(TP0, 100, 102);
      tracker.register(TP0, 103);
      assertEquals(4, tracker.pendingCount(TP0));
    }

    @Test
    void registerBatch_thenProcessAll() {
      tracker.registerBatch(TP0, 100, 102);

      processOneRecord(TP0, 100);
      processOneRecord(TP0, 101);
      processOneRecord(TP0, 102);

      assertEquals(OptionalLong.of(103), tracker.getCommittableOffset(TP0));
      assertEquals(0, tracker.pendingCount(TP0));
    }
  }

  // ── Error: Uninitialized Partition ────────────────────────────

  @Nested
  class UninitializedPartition {

    @Test
    void register_throwsOnUninitialized() {
      assertThrows(IllegalStateException.class, () -> tracker.register(TP_UNKNOWN, 0));
    }

    @Test
    void markInProgress_throwsOnUninitialized() {
      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP_UNKNOWN, 0));
    }

    @Test
    void ack_throwsOnUninitialized() {
      assertThrows(IllegalStateException.class, () -> tracker.ack(TP_UNKNOWN, 0));
    }

    @Test
    void fail_throwsOnUninitialized() {
      assertThrows(IllegalStateException.class, () -> tracker.fail(TP_UNKNOWN, 0));
    }

    @Test
    void getCommittableOffset_returnsEmptyForUninitialized() {
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP_UNKNOWN));
    }

    @Test
    void monitoringMethods_returnZeroForUninitialized() {
      assertEquals(0, tracker.pendingCount(TP_UNKNOWN));
      assertEquals(0, tracker.inProgressCount(TP_UNKNOWN));
      assertEquals(0, tracker.lag(TP_UNKNOWN));
    }
  }

  // ── Error: Invalid State Transitions ─────────────────────────

  @Nested
  class InvalidStateTransitions {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void markInProgress_withoutRegister_throws() {
      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 100));
    }

    @Test
    void markInProgress_offsetOutOfRange_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 999));
    }

    @Test
    void markInProgress_offsetBelowCommittable_throws() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      processOneRecord(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 99));
    }

    @Test
    void ack_withoutMarkInProgress_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.ack(TP0, 100));
    }

    @Test
    void ack_wrongOffset_throws() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.ack(TP0, 999));
    }

    @Test
    void doubleMarkInProgress_throws() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      tracker.markInProgress(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 101));
    }

    @Test
    void register_duplicateOffset_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.register(TP0, 100));
    }

    @Test
    void register_lowerOffset_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.register(TP0, 99));
    }
  }

  // ── Failed State ─────────────────────────────────────────────

  @Nested
  class FailedState {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void fail_blocksSubsequentRegister() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.register(TP0, 101));
    }

    @Test
    void fail_blocksSubsequentMarkInProgress() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 101));
    }

    @Test
    void fail_thenAck_throwsBug1Fix() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.ack(TP0, 100));
    }

    @Test
    void fail_doesNotAdvanceCommittableOffset() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      processOneRecord(TP0, 100);

      tracker.markInProgress(TP0, 101);
      tracker.fail(TP0, 101);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }

    @Test
    void fail_wrongOffset_throws() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.fail(TP0, 999));
    }

    @Test
    void fail_withNothingInProgress_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.fail(TP0, 100));
    }
  }

  // ── Resolve Failure ──────────────────────────────────────────

  @Nested
  class ResolveFailure {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void resolveFailure_resumesProcessing() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      tracker.resolveFailure(TP0, 100);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));

      tracker.markInProgress(TP0, 101);
      tracker.ack(TP0, 101);
      assertEquals(OptionalLong.of(102), tracker.getCommittableOffset(TP0));
    }

    @Test
    void resolveFailure_whenNotFailed_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.resolveFailure(TP0, 100));
    }

    @Test
    void resolveFailure_advancesCommittableOffset() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      tracker.resolveFailure(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }
  }

  // ── Monitoring ───────────────────────────────────────────────

  @Nested
  class Monitoring {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void lag_tracksDistanceBetweenRegisteredAndCommitted() {
      assertEquals(0, tracker.lag(TP0));

      tracker.register(TP0, 100);
      assertEquals(1, tracker.lag(TP0));

      tracker.register(TP0, 101);
      tracker.register(TP0, 102);
      assertEquals(3, tracker.lag(TP0));

      processOneRecord(TP0, 100);
      assertEquals(2, tracker.lag(TP0));

      processOneRecord(TP0, 101);
      processOneRecord(TP0, 102);
      assertEquals(0, tracker.lag(TP0));
    }

    @Test
    void countsAccurateThroughLifecycle() {
      tracker.registerBatch(TP0, 100, 102);
      assertEquals(3, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));

      tracker.markInProgress(TP0, 100);
      assertEquals(2, tracker.pendingCount(TP0));
      assertEquals(1, tracker.inProgressCount(TP0));

      tracker.ack(TP0, 100);
      assertEquals(2, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));
    }
  }

  // ── Multi-Partition ──────────────────────────────────────────

  @Nested
  class MultiPartition {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
      tracker.initPartition(TP1, 200);
      tracker.initPartition(TP2, 300);
    }

    @Test
    void partitionsAreIndependent() {
      tracker.register(TP0, 100);
      tracker.register(TP1, 200);

      processOneRecord(TP0, 100);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP1));
      assertEquals(1, tracker.pendingCount(TP1));
    }

    @Test
    void getAllCommittableOffsets_returnsOnlyPartitionsWithProgress() {
      tracker.register(TP0, 100);
      tracker.register(TP1, 200);
      processOneRecord(TP0, 100);
      processOneRecord(TP1, 200);

      Map<TopicPartition, Long> offsets = tracker.getAllCommittableOffsets();
      assertEquals(2, offsets.size());
      assertEquals(101L, offsets.get(TP0));
      assertEquals(201L, offsets.get(TP1));
      assertNull(offsets.get(TP2));
    }

    @Test
    void clearPartition_doesNotAffectOthers() {
      tracker.register(TP0, 100);
      processOneRecord(TP0, 100);
      tracker.register(TP1, 200);
      processOneRecord(TP1, 200);

      tracker.clearPartition(TP0);

      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.of(201), tracker.getCommittableOffset(TP1));
    }
  }

  // ── Drain ────────────────────────────────────────────────────

  @Nested
  class Drain {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void drain_nonExistentPartition_returnsAllCompleted() {
      PartitionDrainResult result = tracker.drainPartition(TP_UNKNOWN, Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
      assertEquals(0, result.completedCount());
      assertEquals(0, result.abandonedCount());
      assertEquals(OptionalLong.empty(), result.committableOffset());
    }

    @Test
    void drain_noActivity_returnsAllCompleted() {
      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
      assertEquals(0, result.completedCount());
      assertEquals(0, result.abandonedCount());
    }

    @Test
    void drain_afterAllCompleted() {
      tracker.registerBatch(TP0, 100, 102);
      processOneRecord(TP0, 100);
      processOneRecord(TP0, 101);
      processOneRecord(TP0, 102);

      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
      assertEquals(3, result.completedCount());
      assertEquals(0, result.abandonedCount());
      assertEquals(OptionalLong.of(103), result.committableOffset());
    }

    @Test
    void drain_withPendingRecords_reportsAbandoned() {
      tracker.registerBatch(TP0, 100, 104);
      processOneRecord(TP0, 100);

      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofMillis(50));
      assertFalse(result.allCompleted());
      assertEquals(1, result.completedCount());
      assertEquals(4, result.abandonedCount());
      assertEquals(OptionalLong.of(101), result.committableOffset());
    }

    @Test
    void drain_withFailedPartition_returnsImmediately() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      long start = System.nanoTime();
      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofSeconds(5));
      long elapsed = System.nanoTime() - start;

      assertFalse(result.allCompleted());
      assertTrue(
          elapsed < Duration.ofSeconds(1).toNanos(),
          "drain should return immediately on failed partition");
    }

    @Test
    void drain_inProgressCompletesOnAnotherThread() throws Exception {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);

      CountDownLatch drainStarted = new CountDownLatch(1);
      AtomicReference<PartitionDrainResult> resultRef = new AtomicReference<>();

      Thread drainThread =
          new Thread(
              () -> {
                drainStarted.countDown();
                resultRef.set(tracker.drainPartition(TP0, Duration.ofSeconds(5)));
              });
      drainThread.start();

      assertTrue(drainStarted.await(1, TimeUnit.SECONDS));
      Thread.sleep(100);

      tracker.ack(TP0, 100);

      drainThread.join(3000);
      assertFalse(drainThread.isAlive());

      PartitionDrainResult result = resultRef.get();
      assertNotNull(result);
      assertTrue(result.allCompleted());
      assertEquals(1, result.completedCount());
      assertEquals(0, result.abandonedCount());
      assertEquals(OptionalLong.of(101), result.committableOffset());
    }

    @Test
    void drain_inProgressTimesOut() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);

      long start = System.nanoTime();
      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofMillis(100));
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      assertFalse(result.allCompleted());
      assertEquals(1, result.abandonedCount());
      assertTrue(elapsed >= 80, "should have waited near the timeout");
    }

    @Test
    void drain_interrupted_reportsPartialResult() throws Exception {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      processOneRecord(TP0, 100);
      tracker.markInProgress(TP0, 101);

      AtomicReference<PartitionDrainResult> resultRef = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);

      Thread drainThread =
          new Thread(
              () -> {
                started.countDown();
                resultRef.set(tracker.drainPartition(TP0, Duration.ofSeconds(10)));
              });
      drainThread.start();

      assertTrue(started.await(1, TimeUnit.SECONDS));
      Thread.sleep(50);
      drainThread.interrupt();
      drainThread.join(3000);

      PartitionDrainResult result = resultRef.get();
      assertNotNull(result);
      assertFalse(result.allCompleted());
      assertEquals(1, result.completedCount());
      assertEquals(1, result.abandonedCount());
      assertEquals(OptionalLong.of(101), result.committableOffset());
    }

    @Test
    void drain_interrupted_noCommittableOffset() throws Exception {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);

      AtomicReference<PartitionDrainResult> resultRef = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);

      Thread drainThread =
          new Thread(
              () -> {
                started.countDown();
                resultRef.set(tracker.drainPartition(TP0, Duration.ofSeconds(10)));
              });
      drainThread.start();

      assertTrue(started.await(1, TimeUnit.SECONDS));
      Thread.sleep(50);
      drainThread.interrupt();
      drainThread.join(3000);

      PartitionDrainResult result = resultRef.get();
      assertNotNull(result);
      assertFalse(result.allCompleted());
      assertEquals(1, result.abandonedCount());
      assertEquals(OptionalLong.empty(), result.committableOffset());
    }

    @Test
    void drain_withPendingAndInProgress_countsAll() {
      tracker.registerBatch(TP0, 100, 104);
      tracker.markInProgress(TP0, 100);

      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofMillis(50));
      assertFalse(result.allCompleted());
      assertEquals(5, result.abandonedCount());
    }
  }

  // ── Concurrency ──────────────────────────────────────────────

  @Nested
  class Concurrency {

    @Test
    void concurrentRegisterAndProcessOnDifferentPartitions() throws Exception {
      int partitionCount = 10;
      int recordsPerPartition = 100;
      ExecutorService executor = Executors.newFixedThreadPool(partitionCount);

      for (int p = 0; p < partitionCount; p++) {
        TopicPartition tp = new TopicPartition("test", p);
        tracker.initPartition(tp, 0);
      }

      var futures = new java.util.ArrayList<Future<?>>();
      for (int p = 0; p < partitionCount; p++) {
        TopicPartition tp = new TopicPartition("test", p);
        futures.add(
            executor.submit(
                () -> {
                  for (int i = 0; i < recordsPerPartition; i++) {
                    tracker.register(tp, i);
                    tracker.markInProgress(tp, i);
                    tracker.ack(tp, i);
                  }
                }));
      }

      for (Future<?> f : futures) {
        f.get(10, TimeUnit.SECONDS);
      }
      executor.shutdown();

      for (int p = 0; p < partitionCount; p++) {
        TopicPartition tp = new TopicPartition("test", p);
        assertEquals(OptionalLong.of(recordsPerPartition), tracker.getCommittableOffset(tp));
        assertEquals(0, tracker.pendingCount(tp));
        assertEquals(0, tracker.inProgressCount(tp));
      }
    }
  }

  // ── Monotonic Committable Offset ─────────────────────────────

  @Nested
  class MonotonicCommittableOffset {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void committableOffset_neverDecreases() {
      tracker.register(TP0, 100);
      processOneRecord(TP0, 100);
      OptionalLong first = tracker.getCommittableOffset(TP0);

      tracker.register(TP0, 101);
      processOneRecord(TP0, 101);
      OptionalLong second = tracker.getCommittableOffset(TP0);

      assertTrue(second.orElse(0) >= first.orElse(0));
    }
  }

  private void processOneRecord(TopicPartition tp, long offset) {
    tracker.markInProgress(tp, offset);
    tracker.ack(tp, offset);
  }
}
