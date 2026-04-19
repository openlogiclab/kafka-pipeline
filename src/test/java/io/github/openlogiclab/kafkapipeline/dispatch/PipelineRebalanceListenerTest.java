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

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
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
            Duration.ofSeconds(5));
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
  }
}
