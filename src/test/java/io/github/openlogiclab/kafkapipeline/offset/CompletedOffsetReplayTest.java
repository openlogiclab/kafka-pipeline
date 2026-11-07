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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** Recording finished offsets in commit metadata and skipping them on the next assignment. */
class CompletedOffsetReplayTest {

  private static final TopicPartition TP = new TopicPartition("t", 0);

  private static void process(OffsetTracker tracker, long offset) {
    tracker.markInProgress(TP, offset);
    tracker.ack(TP, offset);
  }

  /** First owner: offset 100 is stuck, 101..110 finish. Returns what it commits. */
  private static CommitPoint stuckHeadCommit() {
    OffsetTracker first = new UnorderedOffsetTracker(1000, true);
    first.initPartition(TP, 100, null);
    for (long o = 100; o <= 110; o++) first.register(TP, o);
    for (long o = 101; o <= 110; o++) process(first, o);
    return first.getAllCommitPoints().get(TP);
  }

  @Test
  void stuckHead_commitCarriesFinishedOffsets_nextOwnerSkipsThem() {
    CommitPoint point = stuckHeadCommit();
    assertEquals(100, point.offset());
    assertFalse(point.metadata().isEmpty());

    OffsetTracker second = new UnorderedOffsetTracker(1000, true);
    second.initPartition(TP, 100, point.metadata());
    List<Long> toProcess = new ArrayList<>();
    for (long o = 100; o <= 112; o++) {
      if (second.registerUnlessDone(TP, o)) toProcess.add(o);
    }
    assertEquals(List.of(100L, 111L, 112L), toProcess);

    process(second, 100);
    assertEquals(111, second.getCommittableOffset(TP).getAsLong());
    process(second, 111);
    process(second, 112);
    CommitPoint after = second.getAllCommitPoints().get(TP);
    assertEquals(new CommitPoint(113, ""), after);
  }

  @Test
  void metadataForADifferentOffset_isIgnored() {
    CommitPoint point = stuckHeadCommit();
    OffsetTracker second = new UnorderedOffsetTracker(1000, true);
    second.initPartition(TP, 99, point.metadata());
    for (long o = 99; o <= 110; o++) assertTrue(second.registerUnlessDone(TP, o));
  }

  @Test
  void disabled_neitherRecordsNorSkips() {
    OffsetTracker tracker = new UnorderedOffsetTracker(1000, false);
    tracker.initPartition(TP, 0, null);
    tracker.register(TP, 0);
    tracker.register(TP, 1);
    process(tracker, 1);
    assertTrue(tracker.getAllCommitPoints().isEmpty(), "nothing to commit: head still running");

    CommitPoint point = stuckHeadCommit();
    OffsetTracker second = new UnorderedOffsetTracker(1000, false);
    second.initPartition(TP, 100, point.metadata());
    assertTrue(second.registerUnlessDone(TP, 101));
  }

  @Test
  void uncommitted_changesWhenFinishedSetChanges_evenIfOffsetDoesNot() {
    OffsetTracker tracker = new UnorderedOffsetTracker(1000, true);
    tracker.initPartition(TP, 0, null);
    for (long o = 0; o < 5; o++) tracker.register(TP, o);
    assertTrue(tracker.getUncommittedCommitPoints().isEmpty());

    process(tracker, 2);
    CommitPoint p1 = tracker.getUncommittedCommitPoints().get(TP);
    assertEquals(0, p1.offset());
    tracker.markCommitted(TP, p1);
    assertTrue(tracker.getUncommittedCommitPoints().isEmpty(), "already committed");

    process(tracker, 3);
    CommitPoint p2 = tracker.getUncommittedCommitPoints().get(TP);
    assertEquals(0, p2.offset());
    assertNotEquals(p1.metadata(), p2.metadata());

    process(tracker, 0);
    process(tracker, 1);
    CommitPoint p3 = tracker.getUncommittedCommitPoints().get(TP);
    assertEquals(4, p3.offset());
    assertEquals("", p3.metadata());
  }

  @Test
  void skippedOffsetsNotFetchedYet_survive_aSecondRestart() {
    CommitPoint point = stuckHeadCommit(); // 100 stuck, 101..110 done
    OffsetTracker second = new UnorderedOffsetTracker(1000, true);
    second.initPartition(TP, 100, point.metadata());
    // The second owner only fetches 100..103 and finishes 100 before it too goes away.
    for (long o = 100; o <= 103; o++) second.registerUnlessDone(TP, o);
    process(second, 100);
    CommitPoint again = second.getAllCommitPoints().get(TP);
    // 104..110 were finished by the first owner, so the commit moves past them unfetched.
    assertEquals(new CommitPoint(111, ""), again);
  }

  @Test
  void skippedOffsetsBehindAGap_survive_aSecondRestart() {
    CommitPoint point = stuckHeadCommit(); // 100 stuck, 101..110 done
    OffsetTracker second = new UnorderedOffsetTracker(1000, true);
    second.initPartition(TP, 100, point.metadata());
    for (long o = 100; o <= 103; o++) second.registerUnlessDone(TP, o);
    // 100 is still stuck in the second owner too; 111 (new) finishes.
    second.registerUnlessDone(TP, 111);
    CommitPoint again = second.getAllCommitPoints().get(TP);
    assertEquals(100, again.offset());

    OffsetTracker third = new UnorderedOffsetTracker(1000, true);
    third.initPartition(TP, 100, again.metadata());
    assertTrue(third.registerUnlessDone(TP, 100));
    for (long o = 101; o <= 110; o++) assertFalse(third.registerUnlessDone(TP, o), "offset " + o);
    assertTrue(third.registerUnlessDone(TP, 111), "111 was registered but never finished");
  }

  @Test
  void batchRegistration_leavesOutFinishedOffsets() {
    CommitPoint point = stuckHeadCommit();
    OffsetTracker second = new UnorderedOffsetTracker(1000, true);
    second.initPartition(TP, 100, point.metadata());

    assertArrayEquals(
        new long[] {100}, second.registerBatchUnlessDone(TP, new long[] {100, 101, 102}));
    assertArrayEquals(new long[0], second.registerBatchUnlessDone(TP, new long[] {103, 104}));
    assertArrayEquals(
        new long[] {111, 112}, second.registerBatchUnlessDone(TP, new long[] {110, 111, 112}));
    assertEquals(1, second.pendingCount(TP) - 2);

    second.markBatchInProgress(TP, new long[] {100});
    second.ackBatch(TP, new long[] {100});
    assertEquals(111, second.getCommittableOffset(TP).getAsLong());
  }

  @Test
  void batchRegistration_rejectsDuplicates() {
    CommitPoint point = stuckHeadCommit();
    OffsetTracker second = new UnorderedOffsetTracker(1000, true);
    second.initPartition(TP, 100, point.metadata());
    second.registerBatchUnlessDone(TP, new long[] {100, 101});
    assertThrows(
        IllegalStateException.class, () -> second.registerBatchUnlessDone(TP, new long[] {101}));
    assertThrows(IllegalStateException.class, () -> second.registerUnlessDone(TP, 101));
    assertThrows(
        IllegalArgumentException.class, () -> second.registerBatchUnlessDone(TP, new long[0]));
  }

  @Test
  void skippedOffsets_countTowardTheWindowLimit() {
    CommitPoint point = stuckHeadCommit();
    OffsetTracker second = new UnorderedOffsetTracker(3, true);
    second.initPartition(TP, 100, point.metadata());
    second.registerUnlessDone(TP, 100);
    second.registerUnlessDone(TP, 101);
    second.registerUnlessDone(TP, 102);
    assertThrows(WindowFullException.class, () -> second.registerUnlessDone(TP, 103));
    assertThrows(
        WindowFullException.class, () -> second.registerBatchUnlessDone(TP, new long[] {103}));
  }

  @Test
  void allCommitPoints_includesPartitionThatOnlyHasFinishedOffsets() {
    OffsetTracker tracker = new UnorderedOffsetTracker(1000, true);
    tracker.initPartition(TP, 0, null);
    tracker.register(TP, 0);
    tracker.register(TP, 1);
    assertTrue(tracker.getAllCommitPoints().isEmpty());
    process(tracker, 1);
    Map<TopicPartition, CommitPoint> all = tracker.getAllCommitPoints();
    assertEquals(0, all.get(TP).offset());
    assertFalse(all.get(TP).metadata().isEmpty());
  }

  @Test
  void markCommittedWithOlderPoint_isIgnored() {
    OffsetTracker tracker = new UnorderedOffsetTracker(1000, true);
    tracker.initPartition(TP, 0, null);
    for (long o = 0; o < 3; o++) tracker.register(TP, o);
    for (long o = 0; o < 3; o++) process(tracker, o);
    CommitPoint p = tracker.getUncommittedCommitPoints().get(TP);
    tracker.markCommitted(TP, p);
    tracker.markCommitted(TP, new CommitPoint(1, "kp1:1:x"));
    assertTrue(tracker.getUncommittedCommitPoints().isEmpty());
    tracker.markCommitted(new TopicPartition("other", 0), p); // untracked: no-op
  }

  @Test
  void sparseOffsets_doNotAllocateABitmapTheSizeOfTheGap() {
    // A compacted topic: the next live record after the stuck one is a billion offsets later.
    OffsetTracker tracker = new UnorderedOffsetTracker(1000, true);
    tracker.initPartition(TP, 0, null);
    tracker.register(TP, 0);
    tracker.register(TP, 5);
    tracker.register(TP, 1_000_000_000L);
    process(tracker, 5);
    process(tracker, 1_000_000_000L);

    Runtime rt = Runtime.getRuntime();
    System.gc();
    long before = rt.totalMemory() - rt.freeMemory();
    CommitPoint point = tracker.getAllCommitPoints().get(TP);
    long after = rt.totalMemory() - rt.freeMemory();
    assertTrue(after - before < 16L << 20, "allocated " + ((after - before) >> 20) + " MB");

    // The offsets that fit are still recorded; the far one is just processed again.
    OffsetTracker next = new UnorderedOffsetTracker(1000, true);
    next.initPartition(TP, 0, point.metadata());
    assertTrue(next.registerUnlessDone(TP, 0));
    assertFalse(next.registerUnlessDone(TP, 5));
    assertTrue(next.registerUnlessDone(TP, 1_000_000_000L));
  }
}
