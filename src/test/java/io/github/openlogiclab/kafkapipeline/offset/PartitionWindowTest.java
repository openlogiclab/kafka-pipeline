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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PartitionWindowTest {

  private PartitionWindow window;

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
    void isFailed_reflectsState() {
      assertFalse(window.isFailed());
      window.register(100);
      window.markInProgress(100);
      window.fail(100);
      assertTrue(window.isFailed());

      window.resolveFailure(100);
      assertFalse(window.isFailed());
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
    void failedStateBlocksBatchRegistration() {
      window.register(100);
      window.markInProgress(100);
      window.fail(100);

      assertThrows(IllegalStateException.class, () -> window.registerBatch(101, 105));
    }

    @Test
    void batchExceedsCapacity() {
      PartitionWindow small = new PartitionWindow(0, 3);
      assertThrows(IllegalStateException.class, () -> small.registerBatch(0, 5));
    }

    @Test
    void batchWithDuplicateRollsBack() {
      window.register(102);
      assertThrows(IllegalStateException.class, () -> window.registerBatch(100, 104));
      assertEquals(1, window.pendingCount());
    }
  }

  @Nested
  class ResolveFailure {

    @Test
    void resolveNotAtLeftEdge_doesNotShrink() {
      window.register(100);
      window.register(101);
      window.markInProgress(100);
      window.markInProgress(101);
      window.fail(101);

      window.resolveFailure(101);

      assertEquals(OptionalLong.empty(), window.getCommittableOffset());
      assertFalse(window.isFailed());

      window.ack(100);
      assertEquals(OptionalLong.of(102), window.getCommittableOffset());
    }

    @Test
    void resolveAtLeftEdge_shrinksWindow() {
      window.register(100);
      window.markInProgress(100);
      window.fail(100);

      window.resolveFailure(100);
      assertEquals(OptionalLong.of(101), window.getCommittableOffset());
    }

    @Test
    void resolveNonFailedOffset_throws() {
      window.register(100);
      window.markInProgress(100);
      assertThrows(IllegalStateException.class, () -> window.resolveFailure(100));
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
    void drain_failedPartition_returnsImmediately() {
      window.register(100);
      window.markInProgress(100);
      window.fail(100);

      long start = System.nanoTime();
      PartitionDrainResult result = window.drain(Duration.ofSeconds(5));
      long elapsed = System.nanoTime() - start;

      assertFalse(result.allCompleted());
      assertTrue(elapsed < Duration.ofSeconds(1).toNanos());
    }

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
      window.registerBatch(100, 104);
      window.markBatchInProgress(100, 104);

      assertEquals(0, window.pendingCount());
      assertEquals(5, window.inProgressCount());

      window.ackBatch(100, 104);
      assertEquals(0, window.inProgressCount());
      assertEquals(OptionalLong.of(105), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void markBatchInProgress_partialRange() {
      window.registerBatch(100, 109);
      window.markBatchInProgress(100, 104);

      assertEquals(5, window.pendingCount());
      assertEquals(5, window.inProgressCount());
    }

    @Test
    void ackBatch_partialRange_onlyShrinksContinuous() {
      window.registerBatch(100, 104);
      window.markBatchInProgress(100, 104);

      window.ack(100);
      window.ack(101);
      assertEquals(OptionalLong.of(102), window.getCommittableOffset());

      window.ackBatch(102, 104);
      assertEquals(OptionalLong.of(105), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void markBatchInProgress_onUnregisteredOffset_throws() {
      window.register(100);
      assertThrows(IllegalStateException.class, () -> window.markBatchInProgress(100, 102));
    }

    @Test
    void ackBatch_onNonInProgressOffset_throws() {
      window.registerBatch(100, 102);
      assertThrows(IllegalStateException.class, () -> window.ackBatch(100, 102));
    }

    @Test
    void multipleBatches_sequentially() {
      window.registerBatch(100, 102);
      window.markBatchInProgress(100, 102);
      window.ackBatch(100, 102);
      assertEquals(OptionalLong.of(103), window.getCommittableOffset());

      window.registerBatch(103, 105);
      window.markBatchInProgress(103, 105);
      window.ackBatch(103, 105);
      assertEquals(OptionalLong.of(106), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }

    @Test
    void batchAndSingleRecordInterleaved() {
      window.registerBatch(100, 104);
      window.markBatchInProgress(100, 104);
      window.ackBatch(100, 104);

      window.register(105);
      window.markInProgress(105);
      window.ack(105);

      assertEquals(OptionalLong.of(106), window.getCommittableOffset());
    }

    @Test
    void largeBatch_singleLockAcquisition() {
      int batchSize = 1000;
      window = new PartitionWindow(0, 2000);
      window.registerBatch(0, batchSize - 1);
      window.markBatchInProgress(0, batchSize - 1);
      window.ackBatch(0, batchSize - 1);

      assertEquals(OptionalLong.of(batchSize), window.getCommittableOffset());
      assertEquals(0, window.windowSize());
    }
  }
}
