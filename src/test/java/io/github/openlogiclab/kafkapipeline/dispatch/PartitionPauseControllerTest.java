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

import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionPauseControllerTest {

  private static final TopicPartition TP0 = new TopicPartition("t", 0);
  private static final TopicPartition TP1 = new TopicPartition("t", 1);

  private MockConsumer<String, String> consumer;
  private UnorderedOffsetTracker tracker;
  private PartitionPauseController controller;

  @BeforeEach
  void setUp() {
    consumer = new MockConsumer<>("earliest");
    consumer.assign(List.of(TP0, TP1));
    tracker = new UnorderedOffsetTracker(10); // pause at 8, resume at 5
    tracker.initPartition(TP0, 0);
    tracker.initPartition(TP1, 0);
    controller = new PartitionPauseController(consumer, tracker);
  }

  private void fill(TopicPartition tp, long from, long to) {
    for (long o = from; o < to; o++) tracker.register(tp, o);
  }

  private void complete(TopicPartition tp, long from, long to) {
    for (long o = from; o < to; o++) {
      tracker.markInProgress(tp, o);
      tracker.ack(tp, o);
    }
  }

  @Test
  void pausesOnlyThePartitionWhoseWindowIsNearlyFull() {
    fill(TP0, 0, 8);
    fill(TP1, 0, 3);

    controller.apply(false);

    assertEquals(java.util.Set.of(TP0), consumer.paused());
  }

  @Test
  void resumesWithHysteresis() {
    fill(TP0, 0, 8);
    controller.apply(false);
    assertTrue(consumer.paused().contains(TP0));

    complete(TP0, 0, 2); // window 6: above resume threshold
    controller.apply(false);
    assertTrue(consumer.paused().contains(TP0));

    complete(TP0, 2, 3); // window 5: at resume threshold
    controller.apply(false);
    assertFalse(consumer.paused().contains(TP0));
  }

  @Test
  void throttlePausesAllAndEndingItKeepsWindowPausedPartitionsPaused() {
    fill(TP0, 0, 8);

    controller.apply(true);
    assertEquals(java.util.Set.of(TP0, TP1), consumer.paused());

    controller.apply(false);
    assertEquals(java.util.Set.of(TP0), consumer.paused());
  }

  @Test
  void newlyAssignedPartitionIsPausedWhileThrottled() {
    consumer.assign(List.of(TP0));
    controller.apply(true);
    assertEquals(java.util.Set.of(TP0), consumer.paused());

    consumer.assign(List.of(TP0, TP1));
    controller.apply(true);

    assertTrue(consumer.paused().contains(TP1));
  }

  @Test
  void seekBackMovesPositionAndPausesUntilWindowDrains() {
    consumer.updateBeginningOffsets(Map.of(TP0, 0L, TP1, 0L));
    consumer.seek(TP0, 12);
    fill(TP0, 0, 9);

    controller.seekBack(Map.of(TP0, 9L));

    assertEquals(9L, consumer.position(TP0));
    assertTrue(consumer.paused().contains(TP0));
    assertTrue(controller.isWindowPaused(TP0));

    complete(TP0, 0, 9);
    controller.apply(false);
    assertFalse(consumer.paused().contains(TP0));
  }

  @Test
  void forgetsRevokedPartitions() {
    fill(TP0, 0, 8);
    controller.apply(false);
    assertTrue(controller.isWindowPaused(TP0));

    consumer.assign(List.of(TP1));
    controller.apply(false);

    assertFalse(controller.isWindowPaused(TP0));
  }
}
