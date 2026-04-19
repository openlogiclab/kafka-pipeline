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

class UnorderedOffsetTrackerTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);
  private static final TopicPartition TP2 = new TopicPartition("test", 2);
  private static final TopicPartition TP_UNKNOWN = new TopicPartition("test", 99);

  private UnorderedOffsetTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new UnorderedOffsetTracker();
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
    void clearPartition_thenReinitSucceeds() {
      tracker.initPartition(TP0, 100);
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

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

      tracker.markInProgress(TP0, 100);
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(1, tracker.inProgressCount(TP0));

      tracker.ack(TP0, 100);
      assertEquals(0, tracker.inProgressCount(TP0));
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }

    @Test
    void multipleRecords_inOrder() {
      tracker.registerBatch(TP0, 100, 102);

      for (long offset = 100; offset <= 102; offset++) {
        tracker.markInProgress(TP0, offset);
        tracker.ack(TP0, offset);
      }
      assertEquals(OptionalLong.of(103), tracker.getCommittableOffset(TP0));
    }

    @Test
    void committableOffset_followsKafkaSemantics() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }

    @Test
    void getCommittableOffset_emptyBeforeAnyProgress() {
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
      tracker.register(TP0, 100);
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
    }

    @Test
    void getCommittableOffset_isIdempotent() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }
  }

  // ── Out-of-Order Acks (Core Sliding Window Behavior) ─────────

  @Nested
  class OutOfOrderAcks {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
      tracker.registerBatch(TP0, 100, 104);
    }

    @Test
    void ackLaterOffsetFirst_committableDoesNotAdvancePastGap() {
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      tracker.markInProgress(TP0, 102);

      tracker.ack(TP0, 102);
      assertEquals(
          OptionalLong.empty(),
          tracker.getCommittableOffset(TP0),
          "offset 100 and 101 still in-progress, cannot advance");

      tracker.ack(TP0, 100);
      assertEquals(
          OptionalLong.of(101),
          tracker.getCommittableOffset(TP0),
          "only 100 is contiguous DONE from the start");

      tracker.ack(TP0, 101);
      assertEquals(
          OptionalLong.of(103),
          tracker.getCommittableOffset(TP0),
          "100-102 are all DONE now, jump to 103");
    }

    @Test
    void ackInReverseOrder() {
      for (long offset = 100; offset <= 104; offset++) {
        tracker.markInProgress(TP0, offset);
      }

      tracker.ack(TP0, 104);
      tracker.ack(TP0, 103);
      tracker.ack(TP0, 102);
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));

      tracker.ack(TP0, 101);
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));

      tracker.ack(TP0, 100);
      assertEquals(OptionalLong.of(105), tracker.getCommittableOffset(TP0));
    }

    @Test
    void multipleConcurrentInProgress() {
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      tracker.markInProgress(TP0, 102);

      assertEquals(2, tracker.pendingCount(TP0));
      assertEquals(3, tracker.inProgressCount(TP0));
    }

    @Test
    void committableOnlyAdvancesOnContiguousDone() {
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 102);
      tracker.markInProgress(TP0, 104);

      tracker.ack(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));

      tracker.ack(TP0, 104);
      assertEquals(
          OptionalLong.of(101),
          tracker.getCommittableOffset(TP0),
          "gap at 101, cannot advance past 101");

      tracker.markInProgress(TP0, 101);
      tracker.ack(TP0, 101);
      assertEquals(OptionalLong.of(102), tracker.getCommittableOffset(TP0));

      tracker.ack(TP0, 102);
      assertEquals(
          OptionalLong.of(103),
          tracker.getCommittableOffset(TP0),
          "103 still REGISTERED (pending), stop here");
    }

    @Test
    void eagerShrink_leftEdgeAck() {
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      tracker.markInProgress(TP0, 102);

      tracker.ack(TP0, 101);
      tracker.ack(TP0, 102);

      tracker.ack(TP0, 100);
      assertEquals(
          OptionalLong.of(103),
          tracker.getCommittableOffset(TP0),
          "shrink should have advanced through 100, 101, 102 eagerly on ack(100)");
    }
  }

  // ── Offset Gaps (Compacted Topics) ───────────────────────────

  @Nested
  class OffsetGaps {

    @BeforeEach
    void init() {
      tracker.initPartition(TP0, 100);
    }

    @Test
    void gapInOffsets_committableAdvancesCorrectly() {
      tracker.register(TP0, 100);
      tracker.register(TP0, 101);
      tracker.register(TP0, 105);

      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));

      tracker.markInProgress(TP0, 101);
      tracker.ack(TP0, 101);
      assertEquals(OptionalLong.of(102), tracker.getCommittableOffset(TP0));

      tracker.markInProgress(TP0, 105);
      tracker.ack(TP0, 105);
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
    void registerBatch_duplicateOffset_throwsAndRollsBack() {
      tracker.register(TP0, 102);

      assertThrows(IllegalStateException.class, () -> tracker.registerBatch(TP0, 100, 104));

      assertEquals(1, tracker.pendingCount(TP0), "only the original register(102) should remain");
    }

    @Test
    void multipleBatches_sequential() {
      tracker.registerBatch(TP0, 100, 102);
      tracker.registerBatch(TP0, 103, 105);
      assertEquals(6, tracker.pendingCount(TP0));
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
    void markInProgress_unregisteredOffset_throws() {
      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 100));
    }

    @Test
    void ack_registeredButNotInProgress_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.ack(TP0, 100));
    }

    @Test
    void ack_unknownOffset_throws() {
      assertThrows(IllegalStateException.class, () -> tracker.ack(TP0, 999));
    }

    @Test
    void doubleAck_throws() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

      assertThrows(
          IllegalStateException.class,
          () -> tracker.ack(TP0, 100),
          "offset already removed from window by shrink");
    }

    @Test
    void register_duplicateOffset_throws() {
      tracker.register(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.register(TP0, 100));
    }

    @Test
    void markInProgress_alreadyInProgress_throws() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 100));
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
      tracker.registerBatch(TP0, 100, 101);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.markInProgress(TP0, 101));
    }

    @Test
    void fail_doesNotAdvanceCommittableOffset() {
      tracker.registerBatch(TP0, 100, 102);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

      tracker.markInProgress(TP0, 101);
      tracker.fail(TP0, 101);

      assertEquals(
          OptionalLong.of(101),
          tracker.getCommittableOffset(TP0),
          "FAILED entry at 101 blocks window advance");
    }

    @Test
    void fail_withCompletedRecordsBehind_preservesCommittable() {
      tracker.registerBatch(TP0, 100, 103);
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      tracker.markInProgress(TP0, 102);

      tracker.ack(TP0, 100);
      tracker.ack(TP0, 101);
      tracker.fail(TP0, 102);

      assertEquals(
          OptionalLong.of(102),
          tracker.getCommittableOffset(TP0),
          "100 and 101 are done, 102 is failed, so committable = 102");
    }

    @Test
    void fail_onRegisteredOffset_throws() {
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
    void resolveFailure_allowsWindowToAdvance() {
      tracker.registerBatch(TP0, 100, 102);
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      tracker.markInProgress(TP0, 102);

      tracker.ack(TP0, 100);
      tracker.fail(TP0, 101);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));

      tracker.resolveFailure(TP0, 101);

      tracker.ack(TP0, 102);
      assertEquals(OptionalLong.of(103), tracker.getCommittableOffset(TP0));
    }

    @Test
    void resolveFailure_whenNotFailed_throws() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      assertThrows(IllegalStateException.class, () -> tracker.resolveFailure(TP0, 100));
    }

    @Test
    void resolveFailure_resumesRegistration() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      assertThrows(IllegalStateException.class, () -> tracker.register(TP0, 101));

      tracker.resolveFailure(TP0, 100);

      assertDoesNotThrow(() -> tracker.register(TP0, 101));
    }

    @Test
    void resolveFailure_atLeftEdge_shrinksWindow() {
      tracker.registerBatch(TP0, 100, 102);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      tracker.resolveFailure(TP0, 100);
      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
    }
  }

  // ── Window Max Size ──────────────────────────────────────────

  @Nested
  class WindowMaxSize {

    @Test
    void register_exceedsMaxWindowSize_throws() {
      tracker.initPartition(TP0, 0);

      for (int i = 0; i < PartitionWindow.DEFAULT_MAX_WINDOW_SIZE; i++) {
        tracker.register(TP0, i);
      }

      assertThrows(
          IllegalStateException.class,
          () -> tracker.register(TP0, PartitionWindow.DEFAULT_MAX_WINDOW_SIZE));
    }

    @Test
    void registerBatch_exceedsMaxWindowSize_throws() {
      tracker.initPartition(TP0, 0);

      assertThrows(
          IllegalStateException.class,
          () -> tracker.registerBatch(TP0, 0, PartitionWindow.DEFAULT_MAX_WINDOW_SIZE));
    }

    @Test
    void windowShrink_freesCapacity() {
      tracker.initPartition(TP0, 0);
      int max = PartitionWindow.DEFAULT_MAX_WINDOW_SIZE;

      for (int i = 0; i < max; i++) {
        tracker.register(TP0, i);
      }

      assertThrows(IllegalStateException.class, () -> tracker.register(TP0, max));

      tracker.markInProgress(TP0, 0);
      tracker.ack(TP0, 0);
      tracker.getCommittableOffset(TP0);

      assertDoesNotThrow(
          () -> tracker.register(TP0, max), "after shrink, capacity should be available");
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

      tracker.registerBatch(TP0, 100, 104);
      assertEquals(5, tracker.lag(TP0));

      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);
      tracker.getCommittableOffset(TP0);
      assertEquals(4, tracker.lag(TP0));
    }

    @Test
    void countsAccurateThroughLifecycle() {
      tracker.registerBatch(TP0, 100, 102);
      assertEquals(3, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));

      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      assertEquals(1, tracker.pendingCount(TP0));
      assertEquals(2, tracker.inProgressCount(TP0));

      tracker.ack(TP0, 100);
      assertEquals(1, tracker.pendingCount(TP0));
      assertEquals(1, tracker.inProgressCount(TP0));

      tracker.ack(TP0, 101);
      tracker.markInProgress(TP0, 102);
      tracker.ack(TP0, 102);
      assertEquals(0, tracker.pendingCount(TP0));
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

      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

      assertEquals(OptionalLong.of(101), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP1));
      assertEquals(1, tracker.pendingCount(TP1));
    }

    @Test
    void getAllCommittableOffsets_returnsOnlyPartitionsWithProgress() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

      tracker.register(TP1, 200);
      tracker.markInProgress(TP1, 200);
      tracker.ack(TP1, 200);

      Map<TopicPartition, Long> offsets = tracker.getAllCommittableOffsets();
      assertEquals(2, offsets.size());
      assertEquals(101L, offsets.get(TP0));
      assertEquals(201L, offsets.get(TP1));
      assertNull(offsets.get(TP2));
    }

    @Test
    void failOnePartition_doesNotAffectOthers() {
      tracker.register(TP0, 100);
      tracker.markInProgress(TP0, 100);
      tracker.fail(TP0, 100);

      assertDoesNotThrow(
          () -> {
            tracker.register(TP1, 200);
            tracker.markInProgress(TP1, 200);
            tracker.ack(TP1, 200);
          });
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
    }

    @Test
    void drain_noActivity_returnsAllCompleted() {
      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
    }

    @Test
    void drain_afterAllCompleted() {
      tracker.registerBatch(TP0, 100, 102);
      for (long o = 100; o <= 102; o++) {
        tracker.markInProgress(TP0, o);
        tracker.ack(TP0, o);
      }

      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
      assertEquals(3, result.completedCount());
      assertEquals(0, result.abandonedCount());
      assertEquals(OptionalLong.of(103), result.committableOffset());
    }

    @Test
    void drain_withPendingRecords_reportsAbandoned() {
      tracker.registerBatch(TP0, 100, 104);
      tracker.markInProgress(TP0, 100);
      tracker.ack(TP0, 100);

      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofMillis(50));
      assertFalse(result.allCompleted());
      assertEquals(1, result.completedCount());
      assertEquals(4, result.abandonedCount());
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
      assertTrue(elapsed < Duration.ofSeconds(1).toNanos());
    }

    @Test
    void drain_inProgressCompletesOnAnotherThread() throws Exception {
      tracker.registerBatch(TP0, 100, 102);
      tracker.markInProgress(TP0, 100);
      tracker.markInProgress(TP0, 101);
      tracker.markInProgress(TP0, 102);

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

      tracker.ack(TP0, 102);
      tracker.ack(TP0, 100);
      tracker.ack(TP0, 101);

      drainThread.join(3000);
      assertFalse(drainThread.isAlive());

      PartitionDrainResult result = resultRef.get();
      assertNotNull(result);
      assertTrue(result.allCompleted());
      assertEquals(3, result.completedCount());
      assertEquals(OptionalLong.of(103), result.committableOffset());
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
      assertTrue(elapsed >= 80);
    }

    @Test
    void drain_partiallyCompleted_reportsCorrectCounts() {
      tracker.registerBatch(TP0, 100, 104);
      for (long o = 100; o <= 104; o++) {
        tracker.markInProgress(TP0, o);
      }

      tracker.ack(TP0, 100);
      tracker.ack(TP0, 101);
      tracker.ack(TP0, 102);

      PartitionDrainResult result = tracker.drainPartition(TP0, Duration.ofMillis(50));
      assertFalse(result.allCompleted());
      assertEquals(3, result.completedCount());
      assertEquals(2, result.abandonedCount());
      assertEquals(OptionalLong.of(103), result.committableOffset());
    }
  }

  // ── Concurrency ──────────────────────────────────────────────

  @Nested
  class Concurrency {

    @Test
    void concurrentAcks_allOffsetsCompleteCorrectly() throws Exception {
      tracker.initPartition(TP0, 0);
      int count = 1000;

      for (int i = 0; i < count; i++) {
        tracker.register(TP0, i);
        tracker.markInProgress(TP0, i);
      }

      ExecutorService executor = Executors.newFixedThreadPool(8);
      CountDownLatch latch = new CountDownLatch(count);

      for (int i = 0; i < count; i++) {
        int offset = i;
        executor.submit(
            () -> {
              tracker.ack(TP0, offset);
              latch.countDown();
            });
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
      executor.shutdown();

      assertEquals(OptionalLong.of(count), tracker.getCommittableOffset(TP0));
      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(0, tracker.inProgressCount(TP0));
    }

    @Test
    void concurrentRegisterAndProcessOnDifferentPartitions() throws Exception {
      int partitionCount = 10;
      int recordsPerPartition = 100;
      ExecutorService executor = Executors.newFixedThreadPool(partitionCount);

      for (int p = 0; p < partitionCount; p++) {
        tracker.initPartition(new TopicPartition("test", p), 0);
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
      }
    }
  }

  // ── Rebalance Simulation ─────────────────────────────────────

  @Nested
  class RebalanceSimulation {

    @Test
    void fullRebalanceCycle_revokeAndReassign() {
      tracker.initPartition(TP0, 100);
      tracker.initPartition(TP1, 200);

      tracker.registerBatch(TP0, 100, 102);
      tracker.registerBatch(TP1, 200, 202);

      for (long o = 100; o <= 102; o++) {
        tracker.markInProgress(TP0, o);
        tracker.ack(TP0, o);
      }
      tracker.markInProgress(TP1, 200);
      tracker.ack(TP1, 200);

      PartitionDrainResult drainTP0 = tracker.drainPartition(TP0, Duration.ofSeconds(1));
      PartitionDrainResult drainTP1 = tracker.drainPartition(TP1, Duration.ofMillis(50));

      assertTrue(drainTP0.allCompleted());
      assertFalse(drainTP1.allCompleted());

      assertEquals(OptionalLong.of(103), drainTP0.committableOffset());
      assertEquals(OptionalLong.of(201), drainTP1.committableOffset());

      tracker.clearPartition(TP0);
      tracker.clearPartition(TP1);

      tracker.initPartition(TP0, 103);
      tracker.initPartition(TP1, 201);
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP0));
      assertEquals(OptionalLong.empty(), tracker.getCommittableOffset(TP1));
    }
  }
}
