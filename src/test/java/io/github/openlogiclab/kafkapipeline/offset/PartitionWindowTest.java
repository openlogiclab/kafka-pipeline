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
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PartitionWindowTest {

  private PartitionWindow window;

  private static long[] range(long from, long to) {
    return LongStream.rangeClosed(from, to).toArray();
  }

  @BeforeEach
  void setUp() {
    window = new PartitionWindow(100);
  }

  @Nested
  class Constructor {

    @Test
    void invalidMaxWindowSizeThrows() {
      assertThrows(IllegalArgumentException.class, () -> new PartitionWindow(0, 0));
      assertThrows(IllegalArgumentException.class, () -> new PartitionWindow(0, -5));
    }

    @Test
    void customMaxWindowSize() {
      PartitionWindow small = new PartitionWindow(0, 3);
      small.register(0);
      small.register(1);
      small.register(2);
      assertThrows(IllegalStateException.class, () -> small.register(3));
    }
  }

  @Nested
  class MonitoringMethods {

    @Test
    void windowSize_tracksEntries() {
      assertEquals(0, window.windowSize());
      window.register(100);
      assertEquals(1, window.windowSize());
      window.register(101);
      assertEquals(2, window.windowSize());

      window.markInProgress(100);
      window.ack(100);
      window.getCommittableOffset();
      assertEquals(1, window.windowSize());
    }

    @Test
    void isFull_reflectsCapacity() {
      PartitionWindow small = new PartitionWindow(0, 2);
      assertFalse(small.isFull());
      small.register(0);
      assertFalse(small.isFull());
      small.register(1);
      assertTrue(small.isFull());
    }

    @Test
    void lag_beforeAnyRegistration() {
      assertEquals(0, window.lag());
    }

    @Test
    void lag_afterRegistration() {
      window.register(100);
      window.register(101);
      assertEquals(2, window.lag());
    }

    @Test
    void lag_afterArrayBatchRegistration() {
      long[] offsets = {100, 200, 300};
      window.registerBatch(offsets);
      assertEquals(201, window.lag());
    }

    @Test
    void pendingAndInProgressCounts() {
      window.register(100);
      window.register(101);
      assertEquals(2, window.pendingCount());
      assertEquals(0, window.inProgressCount());

      window.markInProgress(100);
      assertEquals(1, window.pendingCount());
      assertEquals(1, window.inProgressCount());
    }
  }

  @Nested
  class RegisterBatch {

    @Test
    void batchExceedsCapacity() {
      PartitionWindow small = new PartitionWindow(0, 3);
      assertThrows(IllegalStateException.class, () -> small.registerBatch(range(0, 5)));
    }

    @Test
    void batchWithDuplicateRollsBack() {
      window.register(102);
      assertThrows(IllegalStateException.class, () -> window.registerBatch(range(100, 104)));
      assertEquals(1, window.pendingCount());
    }
  }

  @Nested
  class DrainInterrupted {

    @Test
    void interrupted_withCommittableOffset() throws Exception {
      window.register(100);
      window.register(101);
      window.markInProgress(100);
      window.ack(100);
      window.markInProgress(101);

      AtomicReference<PartitionDrainResult> resultRef = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);

      Thread drainThread =
          new Thread(
              () -> {
                started.countDown();
                resultRef.set(window.drain(Duration.ofSeconds(10)));
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
    void interrupted_withoutCommittableOffset() throws Exception {
      window.register(100);
      window.markInProgress(100);

      AtomicReference<PartitionDrainResult> resultRef = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);

      Thread drainThread =
          new Thread(
              () -> {
                started.countDown();
                resultRef.set(window.drain(Duration.ofSeconds(10)));
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
  }

  @Nested
  class DrainEdgeCases {

    @Test
    void drain_noInProgress_completesImmediately() {
      PartitionDrainResult result = window.drain(Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
      assertEquals(0, result.abandonedCount());
      assertEquals(OptionalLong.empty(), result.committableOffset());
    }

    @Test
    void drain_withPendingOnly_reportsAbandoned() {
      window.register(100);
      window.register(101);

      PartitionDrainResult result = window.drain(Duration.ofMillis(50));
      assertFalse(result.allCompleted());
      assertEquals(2, result.abandonedCount());
    }

    @Test
    void drain_allCompleted_reportsSuccess() {
      window.register(100);
      window.markInProgress(100);
      window.ack(100);

      PartitionDrainResult result = window.drain(Duration.ofSeconds(1));
      assertTrue(result.allCompleted());
      assertEquals(1, result.completedCount());
      assertEquals(0, result.abandonedCount());
      assertEquals(OptionalLong.of(101), result.committableOffset());
    }
  }

  @Nested
  class BatchOperations {

    @Test
    void markBatchInProgress_thenAckBatch_advancesWindow() {
      window.registerBatch(range(100, 104));
      window.markBatchInProgress(range(100, 104));

      assertEquals(0, window.pendingCount());
      assertEquals(5, window.inProgressCount());

      window.ackBatch(range(100, 104));
      assertEquals(0, window.inProgressCount());
      assertEquals(OptionalLong.of(105), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void markBatchInProgress_partialRange() {
      window.registerBatch(range(100, 109));
      window.markBatchInProgress(range(100, 104));

      assertEquals(5, window.pendingCount());
      assertEquals(5, window.inProgressCount());
    }

    @Test
    void ackBatch_partialRange_onlyShrinksContinuous() {
      window.registerBatch(range(100, 104));
      window.markBatchInProgress(range(100, 104));

      window.ack(100);
      window.ack(101);
      assertEquals(OptionalLong.of(102), window.getCommittableOffset());

      window.ackBatch(range(102, 104));
      assertEquals(OptionalLong.of(105), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void markBatchInProgress_onUnregisteredOffset_isLenient() {
      window.register(100);
      // Only 100 is registered, trying to mark 100, 101, 102
      int transitioned = window.markBatchInProgress(range(100, 102));
      assertEquals(1, transitioned); // Only 100 transitioned
      assertEquals(1, window.inProgressCount());
    }

    @Test
    void ackBatch_onNonInProgressOffset_isLenient() {
      window.registerBatch(range(100, 102));
      // Not marked in-progress
      int acked = window.ackBatch(range(100, 102));
      assertEquals(0, acked); // None acked because none were IN_PROGRESS
    }

    @Test
    void multipleBatches_sequentially() {
      window.registerBatch(range(100, 102));
      window.markBatchInProgress(range(100, 102));
      window.ackBatch(range(100, 102));
      assertEquals(OptionalLong.of(103), window.getCommittableOffset());

      window.registerBatch(range(103, 105));
      window.markBatchInProgress(range(103, 105));
      window.ackBatch(range(103, 105));
      assertEquals(OptionalLong.of(106), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void batchAndSingleRecordInterleaved() {
      window.registerBatch(range(100, 104));
      window.markBatchInProgress(range(100, 104));
      window.ackBatch(range(100, 104));

      window.register(105);
      window.markInProgress(105);
      window.ack(105);

      assertEquals(OptionalLong.of(106), window.getCommittableOffset());
    }

    @Test
    void largeBatch_singleLockAcquisition() {
      int batchSize = 1000;
      window = new PartitionWindow(0, 2000);
      long[] offsets = range(0, batchSize - 1);
      window.registerBatch(offsets);
      window.markBatchInProgress(offsets);
      window.ackBatch(offsets);

      assertEquals(OptionalLong.of(batchSize), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }
  }

  @Nested
  class ArrayBasedBatchOperations {

    @Test
    void registerBatchWithArray_tracksOnlySpecifiedOffsets() {
      long[] offsets = {100, 105, 110};
      window.registerBatch(offsets);

      assertEquals(3, window.pendingCount());
      assertEquals(3, window.windowSize());
    }

    @Test
    void registerBatchWithArray_nonConsecutiveOffsets_usesMinimalCapacity() {
      PartitionWindow small = new PartitionWindow(0, 100);
      long[] offsets = {100, 5100, 10100};

      assertDoesNotThrow(() -> small.registerBatch(offsets));
      assertEquals(3, small.windowSize());
    }

    @Test
    void markBatchInProgressWithArray_worksWithNonConsecutiveOffsets() {
      long[] offsets = {100, 105, 110};
      window.registerBatch(offsets);
      window.markBatchInProgress(offsets);

      assertEquals(0, window.pendingCount());
      assertEquals(3, window.inProgressCount());
    }

    @Test
    void ackBatchWithArray_worksWithNonConsecutiveOffsets() {
      long[] offsets = {100, 105, 110};
      window.registerBatch(offsets);
      window.markBatchInProgress(offsets);
      window.ackBatch(offsets);

      assertEquals(0, window.inProgressCount());
      assertEquals(OptionalLong.of(111), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void ackBatchWithArray_contiguousFromStart_shrinksCompletely() {
      long[] offsets = {100, 101, 102};
      window.registerBatch(offsets);
      window.markBatchInProgress(offsets);
      window.ackBatch(offsets);

      assertEquals(OptionalLong.of(103), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void arrayBatch_duplicateOffset_rollsBackAllPreviousEntries() {
      window.register(105);
      long[] offsets = {100, 105, 110}; // 105 already exists

      assertThrows(IllegalStateException.class, () -> window.registerBatch(offsets));
      assertEquals(1, window.pendingCount());
    }

    @Test
    void markBatchInProgressWithArray_unregisteredOffset_isLenient() {
      long[] registered = {100, 102};
      window.registerBatch(registered);

      long[] toMark = {100, 101, 102}; // 101 not registered
      int transitioned = window.markBatchInProgress(toMark);
      assertEquals(2, transitioned); // Only 100 and 102 transitioned
    }

    @Test
    void ackBatchWithArray_notInProgress_isLenient() {
      long[] offsets = {100, 105};
      window.registerBatch(offsets);
      // Not marked in-progress

      int acked = window.ackBatch(offsets);
      assertEquals(0, acked);
    }

    @Test
    void mixedArrayAndRangeOperations() {
      long[] offsets1 = {100, 105};
      window.registerBatch(offsets1);
      window.registerBatch(range(110, 112));

      assertEquals(5, window.windowSize());

      window.markBatchInProgress(offsets1);
      assertEquals(2, window.inProgressCount());

      window.markBatchInProgress(range(110, 112));
      assertEquals(5, window.inProgressCount());

      window.ackBatch(offsets1);
      assertEquals(OptionalLong.of(106), window.getCommittableOffset());
      assertEquals(3, window.windowSize());

      window.ackBatch(range(110, 112));
      assertEquals(OptionalLong.of(113), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void widelySpacedOffsets_fullLifecycle() {
      long[] offsets = {1000, 2000, 3000, 4000, 5000};
      window = new PartitionWindow(1000, 100);

      window.registerBatch(offsets);
      assertEquals(5, window.windowSize());

      window.markBatchInProgress(offsets);
      assertEquals(5, window.inProgressCount());

      window.ackBatch(offsets);
      assertEquals(OptionalLong.of(5001), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }
  }

  @Nested
  class FailedStateTracking {

    @Test
    void isFailed_initiallyFalse() {
      assertFalse(window.isFailed());
    }

    @Test
    void markFailed_setsFailedFlag() {
      assertFalse(window.isFailed());
      window.markFailed();
      assertTrue(window.isFailed());
    }

    @Test
    void failureCount_initiallyZero() {
      assertEquals(0, window.failureCount());
    }

    @Test
    void markFailed_incrementsFailureCount() {
      assertEquals(0, window.failureCount());
      window.markFailed();
      assertEquals(1, window.failureCount());
      window.markFailed();
      assertEquals(2, window.failureCount());
    }

    @Test
    void failedState_isObservabilityOnly_doesNotAffectProcessing() {
      window.register(100);
      window.markInProgress(100);

      window.markFailed();
      assertTrue(window.isFailed());

      window.ack(100);
      assertEquals(OptionalLong.of(101), window.getCommittableOffset());
    }
  }
}
