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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/** Whether a partition's start offset is committed decides whether it is worth committing. */
class StartOffsetCommitTest {

  private static final TopicPartition TP = new TopicPartition("t", 0);

  private final SlidingWindowOffsetTracker tracker = new SlidingWindowOffsetTracker(100, false);

  @Test
  void nothingCommitted_theStartOffsetIsCommittedBeforeAnyRecordFinishes() {
    tracker.initPartition(TP, 1000, null);
    tracker.register(TP, 1000); // in flight, not finished

    assertEquals(new CommitPoint(1000, ""), tracker.getUncommittedCommitPoints().get(TP));
    assertEquals(new CommitPoint(1000, ""), tracker.getAllCommitPoints().get(TP));
    assertEquals(1000, tracker.getCommittableOffset(TP).getAsLong());
  }

  @Test
  void onceConfirmed_theStartOffsetIsNotCommittedAgain() {
    tracker.initPartition(TP, 1000, null);
    tracker.markCommitted(TP, new CommitPoint(1000, ""));

    assertNull(tracker.getUncommittedCommitPoints().get(TP));
  }

  @Test
  void startOffsetAlreadyCommitted_nothingToCommitUntilProgress() {
    tracker.initPartition(TP, 1000, new OffsetAndMetadata(1000));
    tracker.register(TP, 1000);

    assertNull(tracker.getUncommittedCommitPoints().get(TP));
    assertNull(tracker.getAllCommitPoints().get(TP));
    assertTrue(tracker.getCommittableOffset(TP).isEmpty());

    tracker.markInProgress(TP, 1000);
    tracker.ack(TP, 1000);
    assertEquals(new CommitPoint(1001, ""), tracker.getUncommittedCommitPoints().get(TP));
  }

  @Test
  void committedElsewhere_theStartOffsetIsCommitted() {
    // The consumer reset or sought away from the committed offset: the group must learn where
    // this consumer actually is.
    tracker.initPartition(TP, 1000, new OffsetAndMetadata(900));

    assertEquals(new CommitPoint(1000, ""), tracker.getUncommittedCommitPoints().get(TP));
  }

  @Test
  void aLateCommitOfAnEarlierWindow_doesNotMarkTheStartOffsetCommitted() {
    tracker.initPartition(TP, 1000, null); // reassigned, starting away from the committed offset
    tracker.markCommitted(TP, new CommitPoint(400, "")); // callback of the earlier window's commit

    assertEquals(new CommitPoint(1000, ""), tracker.getUncommittedCommitPoints().get(TP));
  }
}
