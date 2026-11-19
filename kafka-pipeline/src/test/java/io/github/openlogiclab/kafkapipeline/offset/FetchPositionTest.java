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

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Offsets after the last record that are not records (a transaction's commit marker, aborted or
 * compacted-away offsets) are committed once every registered record is done.
 */
class FetchPositionTest {

  private static final TopicPartition TP = new TopicPartition("t", 0);

  private final SlidingWindowOffsetTracker tracker = new SlidingWindowOffsetTracker(100, false);

  private void process(long offset) {
    tracker.markInProgress(TP, offset);
    tracker.ack(TP, offset);
  }

  @Test
  void trailingMarker_isCommittedOnceTheLastRecordIsDone() {
    tracker.initPartition(TP, 0);
    for (long o = 0; o < 3; o++) tracker.register(TP, o);
    tracker.advanceFetchPosition(TP, 4); // 3 is the transaction's commit marker

    process(0);
    process(1);
    assertEquals(2, tracker.getCommittableOffset(TP).getAsLong(), "record 2 is not done yet");

    process(2);
    assertEquals(4, tracker.getCommittableOffset(TP).getAsLong());
    assertEquals(0, tracker.uncommittedOffsets(TP), "never negative past the last record");
  }

  @Test
  void positionPastOnlyMarkers_isCommittedWithoutAnyRecord() {
    tracker.initPartition(TP, 10);
    tracker.advanceFetchPosition(TP, 500); // an aborted transaction filtered out by read_committed
    assertEquals(500, tracker.getCommittableOffset(TP).getAsLong());
  }

  @Test
  void aPositionThatIsNotAhead_isIgnored() {
    tracker.initPartition(TP, 0);
    tracker.register(TP, 0);
    tracker.advanceFetchPosition(TP, 5);
    tracker.advanceFetchPosition(TP, 2);
    process(0);
    assertEquals(5, tracker.getCommittableOffset(TP).getAsLong());
  }

  @Test
  void uninitializedPartition_isIgnored() {
    assertDoesNotThrow(() -> tracker.advanceFetchPosition(TP, 5));
  }
}
