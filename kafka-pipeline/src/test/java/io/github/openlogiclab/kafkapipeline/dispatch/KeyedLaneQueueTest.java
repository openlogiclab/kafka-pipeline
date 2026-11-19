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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class KeyedLaneQueueTest {

  private static ConsumerRecord<String, String> rec(long offset, String key) {
    return new ConsumerRecord<>("t", 0, offset, key, "v" + offset);
  }

  @Test
  void sameKey_isHandedOutOneAtATimeInOffsetOrder_otherKeysProceed() {
    KeyedLaneQueue<String, String> q = new KeyedLaneQueue<>(10);
    q.offer(rec(0, "a"));
    q.offer(rec(1, "a"));
    q.offer(rec(2, "b"));

    ConsumerRecord<String, String> first = q.poll();
    assertEquals(0, first.offset());
    ConsumerRecord<String, String> second = q.poll();
    assertEquals(2, second.offset(), "a is busy, so b goes next");
    assertNull(q.poll(), "offset 1 must wait for offset 0 of key a");
    assertFalse(q.hasReady());

    q.release(first);
    assertTrue(q.hasReady());
    assertEquals(1, q.poll().offset());
    assertEquals(0, q.size());
  }

  @Test
  void recordArrivingWhileItsKeyIsBusy_waitsForRelease() {
    KeyedLaneQueue<String, String> q = new KeyedLaneQueue<>(10);
    q.offer(rec(0, "a"));
    ConsumerRecord<String, String> running = q.poll();
    q.offer(rec(1, "a"));
    assertNull(q.poll());
    q.release(running);
    assertEquals(1, q.poll().offset());
  }

  @Test
  void nullKeys_areNotOrdered() {
    KeyedLaneQueue<String, String> q = new KeyedLaneQueue<>(10);
    q.offer(rec(0, null));
    q.offer(rec(1, null));
    assertNotNull(q.poll());
    assertNotNull(q.poll(), "null-key records may run in parallel");
    q.release(rec(0, null)); // no-op
  }

  @Test
  void lowerOffsetGoesFirst_betweenKeyedAndUnkeyed() {
    KeyedLaneQueue<String, String> q = new KeyedLaneQueue<>(10);
    q.offer(rec(0, null));
    q.offer(rec(1, "a"));
    q.offer(rec(2, null));
    assertEquals(0, q.poll().offset());
    assertEquals(1, q.poll().offset());
    assertEquals(2, q.poll().offset());
  }

  @Test
  void byteArrayKeys_withEqualContents_areTheSameKey() {
    KeyedLaneQueue<byte[], String> q = new KeyedLaneQueue<>(10);
    q.offer(new ConsumerRecord<>("t", 0, 0, new byte[] {1, 2}, "x"));
    q.offer(new ConsumerRecord<>("t", 0, 1, new byte[] {1, 2}, "y"));
    assertNotNull(q.poll());
    assertNull(q.poll(), "same bytes, same key");
  }

  @Test
  void capacity_drainAndRemaining() {
    KeyedLaneQueue<String, String> q = new KeyedLaneQueue<>(2);
    assertTrue(q.offer(rec(0, "a")));
    assertTrue(q.offer(rec(1, null)));
    assertFalse(q.offer(rec(2, "b")), "full");
    assertEquals(0, q.remainingCapacity());
    assertEquals(2, q.drain().size());
    assertEquals(0, q.size());
    assertFalse(q.hasReady());
    assertNull(q.poll());
  }

  @Test
  void onlyUnkeyedRecordsWaiting_isReady() {
    KeyedLaneQueue<String, String> q = new KeyedLaneQueue<>(10);
    q.offer(new ConsumerRecord<>("t", 0, 0, null, "v"));
    assertTrue(q.hasReady());
  }

  @Test
  void fifoLane_releaseIsANoOp() {
    FIFOLaneQueue<String, String> q = new FIFOLaneQueue<>(2);
    ConsumerRecord<String, String> r = new ConsumerRecord<>("t", 0, 0, "k", "v");
    q.offer(r);
    q.release(r);
    assertEquals(1, q.size());
  }
}
