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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RecordDispatcherTest {

  private static final TopicPartition TP0 = new TopicPartition("test", 0);
  private static final TopicPartition TP1 = new TopicPartition("test", 1);
  private static final TopicPartition TP2 = new TopicPartition("test", 2);

  private RecordDispatcher<String, String> dispatcher;

  @BeforeEach
  void setUp() {
    dispatcher = new RecordDispatcher<>(100);
  }

  private ConsumerRecord<String, String> record(TopicPartition tp, long offset) {
    return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key", "value-" + offset);
  }

  @Nested
  class Construction {

    @Test
    void rejectsZeroCapacity() {
      assertThrows(IllegalArgumentException.class, () -> new RecordDispatcher<>(0));
    }

    @Test
    void rejectsNegativeCapacity() {
      assertThrows(IllegalArgumentException.class, () -> new RecordDispatcher<>(-1));
    }

    @Test
    void startsWithNoPartitions() {
      assertEquals(Set.of(), dispatcher.partitions());
      assertEquals(0, dispatcher.totalQueuedRecords());
    }
  }

  @Nested
  class PartitionManagement {

    @Test
    void addPartition_registersQueue() {
      dispatcher.addPartition(TP0);
      assertEquals(Set.of(TP0), dispatcher.partitions());
    }

    @Test
    void addPartition_idempotent() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP0);
      assertEquals(Set.of(TP0), dispatcher.partitions());
    }

    @Test
    void addMultiplePartitions() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP1);
      dispatcher.addPartition(TP2);
      assertEquals(Set.of(TP0, TP1, TP2), dispatcher.partitions());
    }

    @Test
    void removePartition_removesQueue() {
      dispatcher.addPartition(TP0);
      dispatcher.removePartition(TP0);
      assertEquals(Set.of(), dispatcher.partitions());
    }

    @Test
    void removePartition_returnsAbandonedRecords() {
      dispatcher.addPartition(TP0);
      dispatcher.dispatch(TP0, record(TP0, 0));
      dispatcher.dispatch(TP0, record(TP0, 1));
      List<ConsumerRecord<String, String>> abandoned = dispatcher.removePartition(TP0);
      assertEquals(2, abandoned.size());
    }

    @Test
    void removePartition_returnsEmptyWhenNotPresent() {
      List<ConsumerRecord<String, String>> abandoned = dispatcher.removePartition(TP0);
      assertTrue(abandoned.isEmpty());
    }

    @Test
    void retainOnly_removesUnlisted() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP1);
      dispatcher.addPartition(TP2);

      dispatcher.retainOnly(Set.of(TP0, TP2));
      assertEquals(Set.of(TP0, TP2), dispatcher.partitions());
    }

    @Test
    void retainOnly_stopsPollingRemovedPartitions() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP1);
      dispatcher.dispatch(TP1, record(TP1, 0));

      dispatcher.retainOnly(Set.of(TP0));

      assertEquals(0, dispatcher.totalQueuedRecords());
      assertNull(dispatcher.poll(10, TimeUnit.MILLISECONDS), "removed queue must not be polled");
    }
  }

  @Nested
  class Dispatch {

    @Test
    void dispatch_toRegisteredPartition() {
      dispatcher.addPartition(TP0);
      dispatcher.dispatch(TP0, record(TP0, 0));
      assertEquals(1, dispatcher.totalQueuedRecords());
    }

    @Test
    void dispatch_toUnregisteredPartition_throws() {
      assertThrows(IllegalStateException.class, () -> dispatcher.dispatch(TP0, record(TP0, 0)));
    }

    @Test
    void dispatch_routesToCorrectPartition() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP1);

      dispatcher.dispatch(TP0, record(TP0, 0));
      dispatcher.dispatch(TP0, record(TP0, 1));
      dispatcher.dispatch(TP1, record(TP1, 0));

      assertEquals(2, dispatcher.queuedRecords(TP0));
      assertEquals(1, dispatcher.queuedRecords(TP1));
      assertEquals(3, dispatcher.totalQueuedRecords());
    }

    @Test
    void dispatch_neverBlocks_hasRoomAndHasDrainedTrackCapacity() {
      RecordDispatcher<String, String> small = new RecordDispatcher<>(4);
      small.addPartition(TP0);

      for (int i = 0; i < 4; i++) {
        assertTrue(small.hasRoom(TP0));
        small.dispatch(TP0, record(TP0, i));
      }
      assertFalse(small.hasRoom(TP0));
      assertFalse(small.hasDrained(TP0));
      assertThrows(IllegalStateException.class, () -> small.dispatch(TP0, record(TP0, 4)));
      assertEquals(4, small.totalQueuedRecords());

      small.poll(10, TimeUnit.MILLISECONDS);
      assertTrue(small.hasRoom(TP0));
      assertFalse(small.hasDrained(TP0), "3 of 4 is above half");
      small.poll(10, TimeUnit.MILLISECONDS);
      assertTrue(small.hasDrained(TP0), "2 of 4 is half");
    }

    @Test
    void hasRoom_falseAndHasDrained_trueForUnknownPartition() {
      assertFalse(dispatcher.hasRoom(TP2));
      assertTrue(dispatcher.hasDrained(TP2));
    }
  }

  @Nested
  class Poll {

    @Test
    void poll_returnsDispatchedRecord() {
      dispatcher.addPartition(TP0);
      dispatcher.dispatch(TP0, record(TP0, 42));

      RecordDispatcher.PollResult<String, String> polled =
          dispatcher.poll(10, TimeUnit.MILLISECONDS);
      assertNotNull(polled);
      assertEquals(TP0, polled.partition());
      assertEquals(42, polled.record().offset());
    }

    @Test
    void poll_returnsNullWhenEmpty() {
      dispatcher.addPartition(TP0);
      assertNull(dispatcher.poll(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void poll_drainsMultiplePartitions() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP1);

      dispatcher.dispatch(TP0, record(TP0, 0));
      dispatcher.dispatch(TP1, record(TP1, 0));

      assertNotNull(dispatcher.poll(10, TimeUnit.MILLISECONDS));
      assertNotNull(dispatcher.poll(10, TimeUnit.MILLISECONDS));
      assertNull(dispatcher.poll(10, TimeUnit.MILLISECONDS));
    }

    @Test
    void poll_fifoWithinPartition() {
      dispatcher.addPartition(TP0);
      dispatcher.dispatch(TP0, record(TP0, 10));
      dispatcher.dispatch(TP0, record(TP0, 20));
      dispatcher.dispatch(TP0, record(TP0, 30));

      assertEquals(10, dispatcher.poll(10, TimeUnit.MILLISECONDS).record().offset());
      assertEquals(20, dispatcher.poll(10, TimeUnit.MILLISECONDS).record().offset());
      assertEquals(30, dispatcher.poll(10, TimeUnit.MILLISECONDS).record().offset());
    }

    @Test
    void poll_wakesUpOnDispatch() throws Exception {
      dispatcher.addPartition(TP0);
      AtomicReference<RecordDispatcher.PollResult<String, String>> result = new AtomicReference<>();
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(1);

      Thread worker =
          new Thread(
              () -> {
                started.countDown();
                result.set(dispatcher.poll(5, TimeUnit.SECONDS));
                done.countDown();
              });
      worker.start();

      assertTrue(started.await(1, TimeUnit.SECONDS));
      Thread.sleep(50);
      dispatcher.dispatch(TP0, record(TP0, 99));
      dispatcher.wakeWorkers(1);

      assertTrue(done.await(1, TimeUnit.SECONDS));
      assertNotNull(result.get());
      assertEquals(99, result.get().record().offset());
    }
  }

  @Nested
  class Monitoring {

    @Test
    void totalQueuedRecords_acrossPartitions() {
      dispatcher.addPartition(TP0);
      dispatcher.addPartition(TP1);
      dispatcher.dispatch(TP0, record(TP0, 0));
      dispatcher.dispatch(TP0, record(TP0, 1));
      dispatcher.dispatch(TP1, record(TP1, 0));

      assertEquals(3, dispatcher.totalQueuedRecords());
    }

    @Test
    void queuedRecords_perPartition() {
      dispatcher.addPartition(TP0);
      dispatcher.dispatch(TP0, record(TP0, 0));
      assertEquals(1, dispatcher.queuedRecords(TP0));
      assertEquals(0, dispatcher.queuedRecords(TP1));
    }

    @Test
    void partitions_isImmutableCopy() {
      dispatcher.addPartition(TP0);
      Set<TopicPartition> snapshot = dispatcher.partitions();
      dispatcher.addPartition(TP1);
      assertEquals(1, snapshot.size());
    }
  }

  @Nested
  class Concurrency {

    @Test
    void concurrentDispatchAndPoll() throws Exception {
      RecordDispatcher<String, String> large = new RecordDispatcher<>(10_000);
      large.addPartition(TP0);
      int total = 5_000;
      AtomicInteger dispatched = new AtomicInteger();
      AtomicInteger polled = new AtomicInteger();
      CountDownLatch producerDone = new CountDownLatch(1);

      ExecutorService exec = Executors.newFixedThreadPool(2);
      exec.submit(
          () -> {
            for (int i = 0; i < total; i++) {
              large.dispatch(TP0, record(TP0, i));
              dispatched.incrementAndGet();
            }
            producerDone.countDown();
          });

      exec.submit(
          () -> {
            while (polled.get() < total) {
              RecordDispatcher.PollResult<String, String> r =
                  large.poll(100, TimeUnit.MILLISECONDS);
              if (r != null) polled.incrementAndGet();
            }
          });

      assertTrue(producerDone.await(10, TimeUnit.SECONDS));

      long deadline = System.currentTimeMillis() + 10_000;
      while (polled.get() < dispatched.get() && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }

      exec.shutdownNow();
      exec.awaitTermination(2, TimeUnit.SECONDS);
      assertEquals(dispatched.get(), polled.get());
    }

    @Test
    void multipleWorkersPolling() throws Exception {
      RecordDispatcher<String, String> large = new RecordDispatcher<>(5_000);
      large.addPartition(TP0);
      large.addPartition(TP1);
      int perPartition = 2_000;
      int total = perPartition * 2;

      for (int i = 0; i < perPartition; i++) {
        large.dispatch(TP0, record(TP0, i));
        large.dispatch(TP1, record(TP1, i));
      }
      assertEquals(total, large.totalQueuedRecords());

      AtomicInteger polled = new AtomicInteger();
      int workers = 4;
      ExecutorService exec = Executors.newFixedThreadPool(workers);

      for (int w = 0; w < workers; w++) {
        exec.submit(
            () -> {
              while (polled.get() < total) {
                RecordDispatcher.PollResult<String, String> r =
                    large.poll(100, TimeUnit.MILLISECONDS);
                if (r != null) polled.incrementAndGet();
              }
            });
      }

      long deadline = System.currentTimeMillis() + 10_000;
      while (polled.get() < total && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }

      exec.shutdownNow();
      exec.awaitTermination(2, TimeUnit.SECONDS);
      assertEquals(total, polled.get());
      assertEquals(0, large.totalQueuedRecords());
    }
  }

  @Nested
  class TargetedWakeups {

    private Thread parkedWorker(AtomicInteger got, CountDownLatch registered) {
      Thread t =
          Thread.ofPlatform()
              .start(
                  () -> {
                    registered.countDown();
                    if (dispatcher.poll(10, TimeUnit.SECONDS) != null) got.incrementAndGet();
                  });
      return t;
    }

    private void awaitParked(Thread... threads) throws InterruptedException {
      long deadline = System.currentTimeMillis() + 2_000;
      for (Thread t : threads) {
        while (t.getState() != Thread.State.TIMED_WAITING) {
          assertTrue(System.currentTimeMillis() < deadline, "worker did not park");
          Thread.sleep(5);
        }
      }
    }

    @Test
    void wakeWorkers_wakesNoMoreWorkersThanRecords() throws Exception {
      dispatcher.addPartition(TP0);
      AtomicInteger got = new AtomicInteger();
      CountDownLatch registered = new CountDownLatch(3);
      Thread a = parkedWorker(got, registered);
      Thread b = parkedWorker(got, registered);
      Thread c = parkedWorker(got, registered);
      awaitParked(a, b, c);

      dispatcher.dispatch(TP0, record(TP0, 0));
      dispatcher.wakeWorkers(1);

      long deadline = System.currentTimeMillis() + 2_000;
      while (got.get() < 1 && System.currentTimeMillis() < deadline) Thread.sleep(5);
      assertEquals(1, got.get());
      Thread.sleep(100);
      int stillParked = 0;
      for (Thread t : new Thread[] {a, b, c}) {
        if (t.getState() == Thread.State.TIMED_WAITING) stillParked++;
      }
      assertEquals(2, stillParked, "only one worker should have been woken");

      a.interrupt();
      b.interrupt();
      c.interrupt();
    }

    @Test
    void remainingRecordsAreHandedOnWithoutWaitingForTimeout() throws Exception {
      dispatcher.addPartition(TP0);
      AtomicInteger got = new AtomicInteger();
      CountDownLatch registered = new CountDownLatch(2);
      Thread a = parkedWorker(got, registered);
      Thread b = parkedWorker(got, registered);
      awaitParked(a, b);

      // Two records but only one wakeup: the woken worker sees a record still queued and wakes
      // the other, well before the 10s park timeout.
      dispatcher.dispatch(TP0, record(TP0, 0));
      dispatcher.dispatch(TP0, record(TP0, 1));
      dispatcher.wakeWorkers(1);

      a.join(2_000);
      b.join(2_000);
      assertEquals(2, got.get());
    }

    @Test
    void wakeWorkers_withNoRecordsWakesNobody() throws Exception {
      dispatcher.addPartition(TP0);
      AtomicInteger got = new AtomicInteger();
      Thread a = parkedWorker(got, new CountDownLatch(1));
      awaitParked(a);

      dispatcher.wakeWorkers(0);
      Thread.sleep(100);

      assertEquals(Thread.State.TIMED_WAITING, a.getState());
      a.interrupt();
    }
  }

  @Nested
  class QueueFullWarning {

    @Test
    void loggedOncePerIntervalNotPerFullQueue() {
      java.util.logging.Logger jul =
          java.util.logging.Logger.getLogger(RecordDispatcher.class.getName());
      AtomicInteger warnings = new AtomicInteger();
      java.util.logging.Handler handler =
          new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord r) {
              if (r.getLevel() == java.util.logging.Level.WARNING
                  && String.valueOf(r.getMessage()).startsWith("Dispatch queue full")) {
                warnings.incrementAndGet();
              }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
          };
      jul.addHandler(handler);
      try {
        dispatcher.addPartition(TP0);
        dispatcher.reportFull(TP0);
        dispatcher.reportFull(TP0);
        dispatcher.reportFull(TP0);

        assertEquals(1, warnings.get(), "three full-queue reports should log one warning");
      } finally {
        jul.removeHandler(handler);
      }
    }
  }
}
