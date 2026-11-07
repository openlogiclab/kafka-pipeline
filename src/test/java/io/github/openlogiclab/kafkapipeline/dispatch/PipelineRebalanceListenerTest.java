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
package io.github.openlogiclab.kafkapipeline.dispatch;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PipelineRebalanceListenerTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);

  private UnorderedOffsetTracker tracker;
  private RecordDispatcher<String, String> dispatcher;
  private InFlightCounter counter;
  private MockConsumer<String, String> mockConsumer;
  private AtomicInteger commitSyncCalls;
  private PipelineRebalanceListener listener;

  @BeforeEach
  void setUp() {
    tracker = new UnorderedOffsetTracker();
    dispatcher = new RecordDispatcher<>(100);
    counter = new InFlightCounter();
    mockConsumer = new MockConsumer<>("earliest");
    commitSyncCalls = new AtomicInteger();

    listener =
        new PipelineRebalanceListener(
            tracker,
            dispatcher,
            commitSyncCalls::incrementAndGet,
            counter,
            mockConsumer,
            Duration.ofSeconds(5),
            NoOpMetricsCollector.INSTANCE);
  }

  @Test
  void revokeAfterShutdown_clearsStateWithoutDrainingOrCommitting() {
    mockConsumer.assign(List.of(TP0));
    mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
    listener.onPartitionsAssigned(List.of(TP0));
    tracker.register(TP0, 0L);
    tracker.markInProgress(TP0, 0L); // a record whose worker was already stopped

    listener.shutdown();
    long start = System.nanoTime();
    assertDoesNotThrow(() -> listener.onPartitionsRevoked(List.of(TP0)));

    assertTrue(
        System.nanoTime() - start < java.util.concurrent.TimeUnit.SECONDS.toNanos(1),
        "must not wait drainTimeout for workers that are already stopped");
    assertEquals(0, commitSyncCalls.get(), "final commit already ran before close");
    assertEquals(OffsetTracker.NO_GENERATION, tracker.generation(TP0), "partition cleared");
  }

  @Test
  void lostPartitions_dropStateWithoutDrainingOrCommitting() {
    mockConsumer.assign(List.of(TP0));
    mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
    listener.onPartitionsAssigned(List.of(TP0));
    tracker.register(TP0, 0L);
    tracker.markInProgress(TP0, 0L); // still running; would block a normal revoke's drain
    tracker.register(TP0, 1L);
    dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, 1L, "k", "v"));
    counter.registered(2, 0);

    long start = System.nanoTime();
    listener.onPartitionsLost(List.of(TP0));

    assertTrue(
        System.nanoTime() - start < java.util.concurrent.TimeUnit.SECONDS.toNanos(1),
        "must not wait drainTimeout");
    assertEquals(0, commitSyncCalls.get(), "lost partitions cannot be committed");
    assertFalse(dispatcher.partitions().contains(TP0));
    assertEquals(OffsetTracker.NO_GENERATION, tracker.generation(TP0));
    assertEquals(1, counter.records(), "queued record released; running one still counts");
  }

  @Test
  void lostNothing_isANoOp() {
    listener.onPartitionsLost(List.of());
    assertEquals(0, commitSyncCalls.get());
  }

  @Test
  void assignedQueue_carriesTheTrackersGeneration() {
    mockConsumer.assign(List.of(TP0));
    mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
    listener.onPartitionsAssigned(List.of(TP0));
    dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, 0L, "k", "v"));

    RecordDispatcher.PollResult<String, String> polled =
        dispatcher.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS);
    assertEquals(tracker.generation(TP0), polled.generation());
  }

  @Nested
  class OnPartitionsAssigned {

    @Test
    void assignsPartitionsAndInitializesState() {
      mockConsumer.assign(List.of(TP0, TP1));
      mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L, TP1, 100L));

      listener.onPartitionsAssigned(List.of(TP0, TP1));

      assertEquals(0, tracker.pendingCount(TP0));
      assertEquals(0, tracker.pendingCount(TP1));
      assertTrue(dispatcher.partitions().contains(TP0));
      assertTrue(dispatcher.partitions().contains(TP1));
    }

    @Test
    void emptyAssignment_noOp() {
      listener.onPartitionsAssigned(Collections.emptyList());
      assertTrue(dispatcher.partitions().isEmpty());
    }
  }

  @Nested
  class OnPartitionsRevoked {

    @Test
    void emptyRevocation_noOp() {
      listener.onPartitionsRevoked(Collections.emptyList());
      assertEquals(0, commitSyncCalls.get());
    }

    @Test
    void revokeDrainsAndCommits() {
      mockConsumer.assign(List.of(TP0));
      mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
      listener.onPartitionsAssigned(List.of(TP0));

      tracker.register(TP0, 0);
      tracker.markInProgress(TP0, 0);
      tracker.ack(TP0, 0);
      counter.registered(1, 0);

      listener.onPartitionsRevoked(List.of(TP0));

      assertEquals(1, commitSyncCalls.get());
      assertTrue(dispatcher.partitions().isEmpty());
    }

    @Test
    void revokeWithAbandonedRecordsInDispatcher() {
      mockConsumer.assign(List.of(TP0));
      mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
      listener.onPartitionsAssigned(List.of(TP0));

      tracker.register(TP0, 0);
      tracker.register(TP0, 1);
      counter.registered(2, 0);
      dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, 0L, "k", "v0"));
      dispatcher.dispatch(TP0, new ConsumerRecord<>("test", 0, 1L, "k", "v1"));

      assertEquals(2, dispatcher.queuedRecords(TP0));

      listener.onPartitionsRevoked(List.of(TP0));

      assertEquals(1, commitSyncCalls.get());
      assertEquals(0, dispatcher.queuedRecords(TP0));
    }

    @Test
    void revokeWithNoAbandonedRecords() {
      mockConsumer.assign(List.of(TP0));
      mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
      listener.onPartitionsAssigned(List.of(TP0));

      listener.onPartitionsRevoked(List.of(TP0));

      assertEquals(1, commitSyncCalls.get());
    }

    @Test
    void revokeMultiplePartitions() {
      mockConsumer.assign(List.of(TP0, TP1));
      Map<TopicPartition, Long> offsets = new HashMap<>();
      offsets.put(TP0, 0L);
      offsets.put(TP1, 0L);
      mockConsumer.updateBeginningOffsets(offsets);
      listener.onPartitionsAssigned(List.of(TP0, TP1));

      listener.onPartitionsRevoked(List.of(TP0, TP1));

      assertEquals(1, commitSyncCalls.get());
      assertTrue(dispatcher.partitions().isEmpty());
    }

    @Test
    void drainThatThrows_stillCommitsAndClearsThePartition() {
      // OffsetTracker is sealed, so make the real drain fail: a null timeout throws inside it.
      PipelineRebalanceListener failing =
          new PipelineRebalanceListener(
              tracker,
              dispatcher,
              commitSyncCalls::incrementAndGet,
              counter,
              mockConsumer,
              null,
              NoOpMetricsCollector.INSTANCE);
      mockConsumer.assign(List.of(TP0));
      mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
      failing.onPartitionsAssigned(List.of(TP0));

      failing.onPartitionsRevoked(List.of(TP0));

      assertEquals(1, commitSyncCalls.get());
      assertEquals(OffsetTracker.NO_GENERATION, tracker.generation(TP0), "no longer tracked");
      failing.shutdown();
    }

    @Test
    void revokeWithInProgressRecords_drainCompletes() {
      mockConsumer.assign(List.of(TP0));
      mockConsumer.updateBeginningOffsets(Map.of(TP0, 0L));
      listener.onPartitionsAssigned(List.of(TP0));

      tracker.register(TP0, 0);
      tracker.register(TP0, 1);
      tracker.markInProgress(TP0, 0);
      tracker.markInProgress(TP0, 1);

      // Complete both records before drain
      tracker.ack(TP0, 0);
      tracker.ack(TP0, 1);
      counter.registered(2, 0);

      listener.onPartitionsRevoked(List.of(TP0));

      assertEquals(1, commitSyncCalls.get());
    }
  }

  @Nested
  class Shutdown {

    @Test
    void shutdown_stopsExecutor() {
      assertDoesNotThrow(() -> listener.shutdown());
    }

    @Test
    void shutdown_isIdempotent() {
      listener.shutdown();
      assertDoesNotThrow(() -> listener.shutdown());
    }
  }

  @Nested
  class CommittedMetadataOnAssign {

    private PipelineRebalanceListener replayListener(MockConsumer<String, String> consumer) {
      tracker = new UnorderedOffsetTracker(100, true);
      return new PipelineRebalanceListener(
          tracker,
          dispatcher,
          commitSyncCalls::incrementAndGet,
          counter,
          consumer,
          Duration.ofSeconds(5),
          NoOpMetricsCollector.INSTANCE,
          true);
    }

    /** Metadata of a previous owner whose offset 10 was stuck while 11 and 12 finished. */
    private String finishedElevenAndTwelve() {
      UnorderedOffsetTracker previous = new UnorderedOffsetTracker(100, true);
      previous.initPartition(TP0, 10, null);
      for (long o = 10; o <= 12; o++) previous.register(TP0, o);
      for (long o = 11; o <= 12; o++) {
        previous.markInProgress(TP0, o);
        previous.ack(TP0, o);
      }
      return previous.getAllCommitPoints().get(TP0).metadata();
    }

    @Test
    void metadataForTheResumeOffset_skipsFinishedRecords() {
      mockConsumer.assign(List.of(TP0));
      mockConsumer.commitSync(
          Map.of(
              TP0,
              new org.apache.kafka.clients.consumer.OffsetAndMetadata(
                  10, finishedElevenAndTwelve())));
      mockConsumer.seek(TP0, 10);
      PipelineRebalanceListener l = replayListener(mockConsumer);
      l.onPartitionsAssigned(List.of(TP0));
      assertFalse(tracker.registerUnlessDone(TP0, 11));
      assertTrue(tracker.registerUnlessDone(TP0, 13));
    }

    @Test
    void resumingFromAnotherOffset_ignoresTheMetadata() {
      mockConsumer.assign(List.of(TP0));
      mockConsumer.commitSync(
          Map.of(
              TP0,
              new org.apache.kafka.clients.consumer.OffsetAndMetadata(
                  10, finishedElevenAndTwelve())));
      mockConsumer.seek(TP0, 5); // e.g. an offset reset
      PipelineRebalanceListener l = replayListener(mockConsumer);
      l.onPartitionsAssigned(List.of(TP0));
      assertTrue(tracker.registerUnlessDone(TP0, 11));
    }

    @Test
    void failingToReadCommittedMetadata_assignsWithoutSkipping() {
      MockConsumer<String, String> broken =
          new MockConsumer<>("earliest") {
            @Override
            public synchronized Map<
                    TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>
                committed(java.util.Set<TopicPartition> partitions) {
              throw new org.apache.kafka.common.errors.TimeoutException("coordinator unavailable");
            }
          };
      broken.assign(List.of(TP0));
      broken.updateBeginningOffsets(Map.of(TP0, 0L));
      PipelineRebalanceListener l = replayListener(broken);
      assertDoesNotThrow(() -> l.onPartitionsAssigned(List.of(TP0)));
      assertTrue(tracker.registerUnlessDone(TP0, 0));
    }
  }
}
