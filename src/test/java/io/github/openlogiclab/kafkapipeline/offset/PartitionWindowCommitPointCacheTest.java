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

import java.util.BitSet;
import org.junit.jupiter.api.Test;

/** The commit tick reuses an unchanged commit point but still keeps the window size accurate. */
class PartitionWindowCommitPointCacheTest {

  private static PartitionWindow trackingWindow() {
    return new PartitionWindow(0, 100, 1, true, new BitSet());
  }

  @Test
  void unchangedWindow_reusesCommitPoint() {
    PartitionWindow w = trackingWindow();
    for (long o = 0; o < 4; o++) w.register(o);
    w.markInProgress(2);
    w.ack(2); // offset 0 still pending: 2 is finished beyond the committable offset

    CommitPoint first = w.getCommitPoint().orElseThrow();
    assertFalse(first.metadata().isEmpty());
    assertSame(first, w.getCommitPoint().orElseThrow(), "nothing changed: no rebuild");

    w.markInProgress(3);
    w.ack(3);
    CommitPoint second = w.getCommitPoint().orElseThrow();
    assertNotSame(first, second);
    assertNotEquals(first.metadata(), second.metadata(), "a new finished offset is recorded");
  }

  @Test
  void stopTrackingCompleted_invalidatesCachedPoint() {
    PartitionWindow w = trackingWindow();
    for (long o = 0; o < 3; o++) w.register(o);
    w.markInProgress(1);
    w.ack(1);
    assertFalse(w.getCommitPoint().orElseThrow().metadata().isEmpty());

    w.stopTrackingCompleted();
    assertTrue(
        w.getCommitPoint().map(CommitPoint::metadata).orElse("").isEmpty(),
        "a cached point with metadata must not be reused after tracking stopped");
  }

  @Test
  void headAck_shrinksWindowEvenWhenPointWasCached() {
    PartitionWindow w = trackingWindow();
    for (long o = 0; o < 3; o++) w.register(o);
    for (long o = 0; o < 3; o++) w.markInProgress(o);
    w.ack(1);
    w.ack(2);
    w.getCommitPoint();
    w.ack(0);
    assertEquals(0, w.windowSize(), "every finished entry is shrunk off the window");
    assertEquals(3, w.getCommitPoint().orElseThrow().offset());
  }

  @Test
  void commitStall_growsOnlyWhileRecordsWait() throws Exception {
    PartitionWindow w = new PartitionWindow(0, 100);
    assertEquals(0, w.commitStallMillis(), "empty window is not stalled");
    w.register(0);
    w.markInProgress(0);
    Thread.sleep(30);
    assertTrue(w.commitStallMillis() >= 25);
    w.ack(0);
    assertEquals(0, w.commitStallMillis(), "nothing waits once the head finished");
  }
}
