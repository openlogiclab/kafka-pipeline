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

import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
      c.commitSync();

      assertNotNull(captured.get());
      assertEquals(1L, captured.get().get(TP0).offset());
    }

    @Test
    void emptyOffsetsDoesNotCallConsumer() {
      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(captured, null);

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, broken, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
      c.start();
      c.commitAsync();
      c.stop();

      assertNull(captured.get(), "Should not call commitAsync when no offsets");
    }

    @Test
    void notRunningSkipsCommit() {
      AtomicReference<Map<TopicPartition, OffsetAndMetadata>> captured = new AtomicReference<>();
      Consumer<String, String> spy = new SpyConsumer(null, captured);

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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

      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, broken, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
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
      PeriodicCommitter c =
          new PeriodicCommitter(tracker, mc, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
      c.start();
      assertThrows(IllegalStateException.class, c::start);
      c.stop();
    }

    @Test
    void stopWithoutStart() {
      MockConsumer<String, String> mc = new MockConsumer<>("earliest");
      PeriodicCommitter c =
          new PeriodicCommitter(tracker, mc, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
      assertDoesNotThrow(c::stop);
    }

    @Test
    void stopCompletesEvenIfSchedulerIsSlow() {
      MockConsumer<String, String> mc = new MockConsumer<>("earliest");
      PeriodicCommitter c =
          new PeriodicCommitter(tracker, mc, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE);
      c.start();
      // stop() should complete without hanging even if scheduler is slow
      assertDoesNotThrow(c::stop);
    }
  }

  @Nested
  class MaybeCommitAsync {

    @Test
    void maybeCommitAsyncCommitsWhenFlagIsSet() throws Exception {
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

      PeriodicCommitter c =
          new PeriodicCommitter(tracker, spy, Duration.ofMillis(50), NoOpMetricsCollector.INSTANCE);
      c.start();

      // Wait for the scheduler to signal commit due
      Thread.sleep(100);

      // Poll loop calls maybeCommitAsync, which should trigger the commit
      c.maybeCommitAsync();
      c.stop();

      assertEquals(1, asyncCalls.get(), "Expected exactly 1 commit from maybeCommitAsync");
    }

    @Test
    void maybeCommitAsyncDoesNothingWhenFlagNotSet() {
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

      // Use a very long interval so the flag won't be set
      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, spy, Duration.ofMinutes(60), NoOpMetricsCollector.INSTANCE);
      c.start();

      // Immediately call maybeCommitAsync before scheduler has a chance to set flag
      c.maybeCommitAsync();
      c.stop();

      assertEquals(0, asyncCalls.get(), "Expected no commits when flag not set");
    }

    @Test
    void maybeCommitAsyncClearsFlag() throws Exception {
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

      PeriodicCommitter c =
          new PeriodicCommitter(tracker, spy, Duration.ofMillis(50), NoOpMetricsCollector.INSTANCE);
      c.start();

      // Wait for flag to be set (scheduler fires every 50ms, wait 200ms to be safe)
      Thread.sleep(200);

      // First call consumes the flag
      c.maybeCommitAsync();
      int firstCount = asyncCalls.get();

      // Second immediate call should not commit (flag cleared)
      c.maybeCommitAsync();
      int secondCount = asyncCalls.get();

      c.stop();

      assertEquals(1, firstCount, "First maybeCommitAsync should commit");
      assertEquals(1, secondCount, "Second maybeCommitAsync should not commit (flag cleared)");
    }

    @Test
    void schedulerSetsCommitDueFlagButDoesNotCallConsumer() throws Exception {
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

      PeriodicCommitter c =
          new PeriodicCommitter(tracker, spy, Duration.ofMillis(30), NoOpMetricsCollector.INSTANCE);
      c.start();

      // Let the scheduler fire multiple times
      Thread.sleep(150);

      // Without calling maybeCommitAsync, no commits should have happened
      assertEquals(0, asyncCalls.get(), "Scheduler should only set flag, not call consumer");

      // Now poll loop calls maybeCommitAsync
      c.maybeCommitAsync();
      c.stop();

      // Should have exactly 1 commit (flag set multiple times but only 1 actual commit)
      assertEquals(
          1, asyncCalls.get(), "Only 1 commit should occur when poll loop calls maybeCommitAsync");
    }
  }

  @Nested
  class Stop {

    @Test
    void stop_setsRunningToFalse() throws Exception {
      tracker.initPartition(TP0, 0);
      MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, consumer, Duration.ofMinutes(60), NoOpMetricsCollector.INSTANCE);
      c.start();
      c.stop();

      // Calling stop again should be a no-op
      assertDoesNotThrow(() -> c.stop());
    }

    @Test
    void stop_interruptHandling() throws Exception {
      tracker.initPartition(TP0, 0);
      MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
      PeriodicCommitter c =
          new PeriodicCommitter(
              tracker, consumer, Duration.ofMinutes(60), NoOpMetricsCollector.INSTANCE);
      c.start();

      Thread.currentThread().interrupt();
      c.stop();

      assertTrue(Thread.currentThread().isInterrupted());
      Thread.interrupted();
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
