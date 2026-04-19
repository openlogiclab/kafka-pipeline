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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PeriodicCommitterTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);

  private UnorderedOffsetTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new UnorderedOffsetTracker();
  }

  private void completeRecord(TopicPartition tp, long offset) {
    tracker.register(tp, offset);
    tracker.markInProgress(tp, offset);
    tracker.ack(tp, offset);
  }

  @Nested
  class CommitSync {

    @Test
    void commitsOffsetsForCompletedRecords() {
      tracker.initPartition(TP0, 0);
      completeRecord(TP0, 0);

      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(captured, null);

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.commitSync();

      assertNotNull(captured.get());
      assertEquals(1L, captured.get().get(TP0).offset());
    }

    @Test
    void emptyOffsetsDoesNotCallConsumer() {
      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(captured, null);

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.commitSync();

      assertNull(captured.get(), "Should not call commitSync when no offsets to commit");
    }

    @Test
    void multiPartitionCommit() {
      tracker.initPartition(TP0, 0);
      tracker.initPartition(TP1, 10);
      completeRecord(TP0, 0);
      completeRecord(TP1, 10);

      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(captured, null);

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.commitSync();

      assertEquals(1L, captured.get().get(TP0).offset());
      assertEquals(11L, captured.get().get(TP1).offset());
    }

    @Test
    void exceptionIsCaughtAndLogged() {
      tracker.initPartition(TP0, 0);
      completeRecord(TP0, 0);

      Consumer<String, String> broken =
          new SpyConsumer(null, null) {
            @Override
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
              throw new RuntimeException("Broker unavailable");
            }
          };

      PeriodicCommitter c = new PeriodicCommitter(tracker, broken, Duration.ofSeconds(60));
      assertDoesNotThrow(c::commitSync);
    }
  }

  @Nested
  class CommitAsync {

    @Test
    void commitsOffsetsAsynchronously() {
      tracker.initPartition(TP0, 0);
      completeRecord(TP0, 0);

      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(null, captured);

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.start();
      c.commitAsync();
      c.stop();

      assertNotNull(captured.get());
      assertEquals(1L, captured.get().get(TP0).offset());
    }

    @Test
    void emptyOffsetsSkipsCommit() {
      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(null, captured);

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.start();
      c.commitAsync();
      c.stop();

      assertNull(captured.get(), "Should not call commitAsync when no offsets");
    }

    @Test
    void notRunningSkipsCommit() {
      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(null, captured);

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.commitAsync();

      assertNull(captured.get(), "commitAsync should skip when not running");
    }

    @Test
    void callbackHandlesException() {
      tracker.initPartition(TP0, 0);
      completeRecord(TP0, 0);

      Consumer<String, String> spy =
          new SpyConsumer(null, null) {
            @Override
            public void commitAsync(
                Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
              callback.onComplete(offsets, new RuntimeException("Callback error"));
            }
          };

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofSeconds(60));
      c.start();
      assertDoesNotThrow(c::commitAsync);
      c.stop();
    }

    @Test
    void exceptionInCommitAsyncIsCaught() {
      tracker.initPartition(TP0, 0);
      completeRecord(TP0, 0);

      Consumer<String, String> broken =
          new SpyConsumer(null, null) {
            @Override
            public void commitAsync(
                Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
              throw new RuntimeException("Commit async explosion");
            }
          };

      PeriodicCommitter c = new PeriodicCommitter(tracker, broken, Duration.ofSeconds(60));
      c.start();
      assertDoesNotThrow(c::commitAsync);
      c.stop();
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void doubleStartThrows() {
      MockConsumer<String, String> mc = new MockConsumer<>("earliest");
      PeriodicCommitter c = new PeriodicCommitter(tracker, mc, Duration.ofSeconds(60));
      c.start();
      assertThrows(IllegalStateException.class, c::start);
      c.stop();
    }

    @Test
    void periodicCommitFires() throws Exception {
      tracker.initPartition(TP0, 0);
      completeRecord(TP0, 0);

      AtomicInteger asyncCalls = new AtomicInteger();
      Consumer<String, String> spy =
          new SpyConsumer(null, null) {
            @Override
            public void commitAsync(
                Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
              asyncCalls.incrementAndGet();
              if (callback != null) callback.onComplete(offsets, null);
            }
          };

      PeriodicCommitter c = new PeriodicCommitter(tracker, spy, Duration.ofMillis(50));
      c.start();
      Thread.sleep(250);
      c.stop();

      assertTrue(asyncCalls.get() >= 2, "Expected at least 2 periodic commits, got " + asyncCalls);
    }

    @Test
    void stopWithoutStart() {
      MockConsumer<String, String> mc = new MockConsumer<>("earliest");
      PeriodicCommitter c = new PeriodicCommitter(tracker, mc, Duration.ofSeconds(60));
      assertDoesNotThrow(c::stop);
    }
  }

  private static class SpyConsumer extends MockConsumer<String, String> {
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> syncCapture;
    private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> asyncCapture;

    SpyConsumer(
        AtomicReference<Map<TopicPartition, OffsetAndMetadata>> syncCapture,
        AtomicReference<Map<TopicPartition, OffsetAndMetadata>> asyncCapture) {
      super("earliest");
      this.syncCapture = syncCapture;
      this.asyncCapture = asyncCapture;
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      if (syncCapture != null) syncCapture.set(new HashMap<>(offsets));
    }

    @Override
    public void commitAsync(
        Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      if (asyncCapture != null) asyncCapture.set(new HashMap<>(offsets));
      if (callback != null) callback.onComplete(offsets, null);
    }
  }
}
