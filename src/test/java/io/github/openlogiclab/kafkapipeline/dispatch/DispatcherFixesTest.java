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

import io.github.openlogiclab.kafkapipeline.ProcessingOrder;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class DispatcherFixesTest {

  private static final TopicPartition TP0 = new TopicPartition("t", 0);

  @Test
  void stateOfRevokedPartition_isDroppedWhenItsLastRecordFinishes() {
    RecordDispatcher<String, String> d = new RecordDispatcher<>(10, 0, 4, ProcessingOrder.KEY);
    d.addPartition(TP0, 1);
    d.dispatch(TP0, new ConsumerRecord<>("t", 0, 0, "A", "v"));
    RecordDispatcher.PollResult<String, String> running = d.poll(10, TimeUnit.MILLISECONDS);
    d.removePartition(TP0);
    assertEquals(1, d.retainedPartitionState(), "kept while the record runs");

    d.release(running); // never assigned back
    assertEquals(0, d.retainedPartitionState(), "dropped without waiting for a re-assignment");
  }

  @Test
  void stateIsKeptWhenThePartitionCameBack() {
    RecordDispatcher<String, String> d =
        new RecordDispatcher<>(10, 0, 4, ProcessingOrder.PARTITION);
    d.addPartition(TP0, 1);
    d.dispatch(TP0, new ConsumerRecord<>("t", 0, 0, "A", "v"));
    RecordDispatcher.PollResult<String, String> running = d.poll(10, TimeUnit.MILLISECONDS);
    d.removePartition(TP0);
    d.addPartition(TP0, 2);
    d.release(running);
    assertEquals(1, d.retainedPartitionState(), "the new assignment still uses its state");
  }

  @Test
  void fifoLaneQueue_handsOutInOrderAndRespectsCapacity() {
    FIFOLaneQueue<String, String> q = new FIFOLaneQueue<>(2);
    ConsumerRecord<String, String> a = new ConsumerRecord<>("t", 0, 0, "k", "a");
    ConsumerRecord<String, String> b = new ConsumerRecord<>("t", 0, 1, "k", "b");
    assertTrue(q.offer(a));
    assertTrue(q.offer(b));
    assertFalse(q.offer(new ConsumerRecord<>("t", 0, 2, "k", "c")), "full");
    assertEquals(0, q.remainingCapacity());
    assertTrue(q.hasReady());
    assertSame(a, q.poll());
    assertFalse(q.release(a), "records never wait on each other");
    assertEquals(List.of(b), q.drain());
    assertEquals(0, q.size());
    assertNull(q.poll());
  }
}
