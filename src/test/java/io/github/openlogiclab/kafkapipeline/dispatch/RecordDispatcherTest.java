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
    void dispatch_blocksWhenQueueFull() throws Exception {
      RecordDispatcher<String, String> small = new RecordDispatcher<>(2);
      small.addPartition(TP0);

      small.dispatch(TP0, record(TP0, 0));
      small.dispatch(TP0, record(TP0, 1));
      assertEquals(2, small.totalQueuedRecords());

      CountDownLatch blocked = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(1);
      Thread producer =
          new Thread(
              () -> {
                blocked.countDown();
                small.dispatch(TP0, record(TP0, 2));
                done.countDown();
              });
      producer.start();

      assertTrue(blocked.await(1, TimeUnit.SECONDS));
      Thread.sleep(100);
      assertFalse(done.await(200, TimeUnit.MILLISECONDS), "dispatch should be blocking");

      small.poll(10, TimeUnit.MILLISECONDS);
      assertTrue(done.await(1, TimeUnit.SECONDS), "dispatch should unblock after poll frees space");
      assertEquals(2, small.totalQueuedRecords());
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
      dispatcher.wakeWorkers();

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
}
