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
package io.github.openlogiclab.kafkapipeline.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.PipelineMetrics;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureController;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.backpressure.RecordCountSensor;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PipelineMetricsCollectorTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);

  private InFlightCounter inFlightCounter;
  private BackpressureController backpressureController;
  private OffsetTracker offsetTracker;
  private PipelineMetricsCollector collector;

  @BeforeEach
  void setUp() {
    inFlightCounter = new InFlightCounter();
    BackpressureConfig bpConfig = BackpressureConfig.builder().build();
    backpressureController =
        new BackpressureController(bpConfig, new RecordCountSensor(bpConfig, inFlightCounter));
    offsetTracker = new UnorderedOffsetTracker();
    collector =
        new PipelineMetricsCollector(inFlightCounter, backpressureController, offsetTracker);
  }

  @Nested
  class ThroughputCounters {

    @Test
    void recordProcessedIncrements() {
      collector.recordProcessed(1);
      collector.recordProcessed(5);
      PipelineMetrics m = collector.snapshot();
      assertEquals(6, m.recordsProcessed());
    }

    @Test
    void recordFailedIncrements() {
      collector.recordFailed();
      collector.recordFailed();
      assertEquals(2, collector.snapshot().recordsFailed());
    }

    @Test
    void recordSkippedIncrements() {
      collector.recordSkipped();
      assertEquals(1, collector.snapshot().recordsSkipped());
    }

    @Test
    void pollAndEmptyPollCounters() {
      collector.recordPoll();
      collector.recordPoll();
      collector.recordPoll();
      collector.recordEmptyPoll();
      collector.recordEmptyPoll();

      PipelineMetrics m = collector.snapshot();
      assertEquals(3, m.pollCount());
      assertEquals(2, m.emptyPollCount());
    }
  }

  @Nested
  class PressureGauges {

    @Test
    void inFlightGaugesReflectCounter() {
      inFlightCounter.registered(5, 1024);
      PipelineMetrics m = collector.snapshot();
      assertEquals(5, m.inFlightRecords());
      assertEquals(1024, m.inFlightBytes());

      inFlightCounter.completed(3, 512);
      m = collector.snapshot();
      assertEquals(2, m.inFlightRecords());
      assertEquals(512, m.inFlightBytes());
    }

    @Test
    void backpressureStatusReflectsController() {
      PipelineMetrics m = collector.snapshot();
      assertEquals(BackpressureStatus.OK, m.backpressureStatus());
    }

    @Test
    void throttleCountIncrements() {
      collector.recordThrottle();
      collector.recordThrottle();
      assertEquals(2, collector.snapshot().throttleCount());
    }

    @Test
    void partitionLagsReflectOffsetTracker() {
      offsetTracker.initPartition(TP0, 0);
      offsetTracker.register(TP0, 0);
      offsetTracker.register(TP0, 1);
      offsetTracker.register(TP0, 2);
      collector.partitionAssigned(TP0);

      PipelineMetrics m = collector.snapshot();
      assertTrue(m.partitionLags().containsKey(TP0));
      assertTrue(m.partitionLags().get(TP0) >= 0);
    }
  }

  @Nested
  class ErrorCounters {

    @Test
    void retryAttemptsIncrements() {
      collector.recordRetry();
      collector.recordRetry();
      collector.recordRetry();
      assertEquals(3, collector.snapshot().retryAttempts());
    }

    @Test
    void dlqCounters() {
      collector.recordDlqSuccess();
      collector.recordDlqSuccess();
      collector.recordDlqFailure();

      PipelineMetrics m = collector.snapshot();
      assertEquals(2, m.dlqSuccesses());
      assertEquals(1, m.dlqFailures());
    }

    @Test
    void partitionFailureIncrements() {
      collector.recordPartitionFailure();
      assertEquals(1, collector.snapshot().partitionFailures());
    }

    @Test
    void commitCounters() {
      collector.recordCommitSuccess();
      collector.recordCommitSuccess();
      collector.recordCommitFailure();

      PipelineMetrics m = collector.snapshot();
      assertEquals(2, m.commitSuccesses());
      assertEquals(1, m.commitFailures());
    }
  }

  @Nested
  class RebalanceCounters {

    @Test
    void rebalanceCountIncrements() {
      collector.recordRebalance();
      collector.recordRebalance();
      assertEquals(2, collector.snapshot().rebalanceCount());
    }

    @Test
    void drainTimeoutIncrements() {
      collector.recordDrainTimeout();
      assertEquals(1, collector.snapshot().drainTimeouts());
    }

    @Test
    void recordsAbandonedAccumulates() {
      collector.recordAbandoned(5);
      collector.recordAbandoned(3);
      assertEquals(8, collector.snapshot().recordsAbandoned());
    }
  }

  @Nested
  class PartitionTracking {

    @Test
    void assignedPartitionsTracksAssignAndRevoke() {
      collector.partitionAssigned(TP0);
      collector.partitionAssigned(TP1);
      assertEquals(Set.of(TP0, TP1), collector.snapshot().assignedPartitions());

      collector.partitionRevoked(TP0);
      assertEquals(Set.of(TP1), collector.snapshot().assignedPartitions());
    }

    @Test
    void assignedPartitionsSnapshotIsImmutable() {
      collector.partitionAssigned(TP0);
      Set<TopicPartition> snapshot = collector.snapshot().assignedPartitions();
      assertThrows(UnsupportedOperationException.class, () -> snapshot.add(TP1));
    }
  }

  @Nested
  class SnapshotConsistency {

    @Test
    void freshCollectorReturnsZeroSnapshot() {
      PipelineMetrics m = collector.snapshot();
      assertEquals(0, m.recordsProcessed());
      assertEquals(0, m.recordsFailed());
      assertEquals(0, m.recordsSkipped());
      assertEquals(0, m.pollCount());
      assertEquals(0, m.emptyPollCount());
      assertEquals(0, m.inFlightRecords());
      assertEquals(0, m.inFlightBytes());
      assertEquals(BackpressureStatus.OK, m.backpressureStatus());
      assertEquals(0, m.throttleCount());
      assertTrue(m.partitionLags().isEmpty());
      assertEquals(0, m.retryAttempts());
      assertEquals(0, m.dlqSuccesses());
      assertEquals(0, m.dlqFailures());
      assertEquals(0, m.partitionFailures());
      assertEquals(0, m.commitSuccesses());
      assertEquals(0, m.commitFailures());
      assertEquals(0, m.rebalanceCount());
      assertEquals(0, m.drainTimeouts());
      assertEquals(0, m.recordsAbandoned());
      assertTrue(m.assignedPartitions().isEmpty());
    }

    @Test
    void multipleSnapshotsAreIndependent() {
      collector.recordProcessed(10);
      PipelineMetrics first = collector.snapshot();

      collector.recordProcessed(5);
      PipelineMetrics second = collector.snapshot();

      assertEquals(10, first.recordsProcessed());
      assertEquals(15, second.recordsProcessed());
    }
  }

  @Nested
  class ThreadSafety {

    @Test
    void concurrentIncrementsProduceCorrectTotals() throws Exception {
      int threads = 8;
      int incrementsPerThread = 10_000;
      long expectedTotal = (long) threads * incrementsPerThread;

      ExecutorService exec = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        exec.submit(
            () -> {
              try {
                start.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              for (int i = 0; i < incrementsPerThread; i++) {
                collector.recordProcessed(1);
                collector.recordPoll();
                collector.recordRetry();
              }
              done.countDown();
            });
      }

      start.countDown();
      assertTrue(done.await(10, TimeUnit.SECONDS), "Threads did not finish in time");
      exec.shutdownNow();

      PipelineMetrics m = collector.snapshot();
      assertEquals(expectedTotal, m.recordsProcessed());
      assertEquals(expectedTotal, m.pollCount());
      assertEquals(expectedTotal, m.retryAttempts());
    }
  }
}
