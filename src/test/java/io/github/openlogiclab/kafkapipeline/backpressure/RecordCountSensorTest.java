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
package io.github.openlogiclab.kafkapipeline.backpressure;

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RecordCountSensorTest {

  private static final BackpressureConfig CONFIG = new BackpressureConfig(100, 60, 500, true);

  private InFlightCounter counter;
  private RecordCountSensor sensor;

  @BeforeEach
  void setUp() {
    counter = new InFlightCounter();
    sensor = new RecordCountSensor(CONFIG, counter);
  }

  @Nested
  class Counter {

    @Test
    void startsAtZero() {
      assertEquals(0, counter.records());
    }

    @Test
    void registered_increments() {
      counter.registered(1, 0);
      assertEquals(1, counter.records());

      counter.registered(1, 0);
      assertEquals(2, counter.records());
    }

    @Test
    void completed_decrements() {
      counter.registered(2, 0);
      counter.completed(1, 0);
      assertEquals(1, counter.records());
    }

    @Test
    void batchRegistered() {
      counter.registered(50, 0);
      assertEquals(50, counter.records());
    }

    @Test
    void batchCompleted() {
      counter.registered(100, 0);
      counter.completed(40, 0);
      assertEquals(60, counter.records());
    }
  }

  @Nested
  class Hysteresis {

    @Test
    void belowHighWatermark_isOK() {
      counter.registered(99, 0);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void atHighWatermark_triggersThrottle() {
      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void aboveHighWatermark_staysThrottle() {
      counter.registered(120, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void throttled_staysThrottleAboveLowWatermark() {
      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(39, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void throttled_resumesAtLowWatermark() {
      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(40, 0);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void throttled_resumesBelowLowWatermark() {
      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(50, 0);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void fullCycle_ok_throttle_ok() {
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());

      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(50, 0);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());

      counter.registered(30, 0);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());

      counter.registered(25, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void hysteresisZone_noFlapping() {
      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(10, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.registered(5, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(20, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.registered(10, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }
  }

  @Nested
  class Critical {

    @Test
    void atCriticalThreshold_isCritical() {
      counter.registered(500, 0);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());
    }

    @Test
    void aboveCriticalThreshold_isCritical() {
      counter.registered(600, 0);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());
    }

    @Test
    void critical_dropsToThrottle_staysThrottled() {
      counter.registered(500, 0);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());

      counter.completed(410, 0);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void critical_dropsBelowLow_resumesOK() {
      counter.registered(500, 0);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());

      counter.completed(450, 0);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }
  }

  @Nested
  class Disabled {

    @Test
    void disabledConfig_alwaysOK() {
      InFlightCounter c = new InFlightCounter();
      RecordCountSensor disabledSensor = new RecordCountSensor(BackpressureConfig.disabled(), c);
      c.registered(999_999, 0);
      assertEquals(BackpressureStatus.OK, disabledSensor.currentStatus());
    }

    @Test
    void disabledConfig_counterStillWorks() {
      InFlightCounter c = new InFlightCounter();
      new RecordCountSensor(BackpressureConfig.disabled(), c);
      c.registered(100, 0);
      assertEquals(100, c.records());
    }
  }

  @Nested
  class Metadata {

    @Test
    void name_isRecordCount() {
      assertEquals("record-count", sensor.name());
    }

    @Test
    void statusDetail_includesCountAndThreshold() {
      counter.registered(42, 0);
      String detail = sensor.statusDetail();
      assertTrue(detail.contains("42"));
      assertTrue(detail.contains("100"));
    }

    @Test
    void statusDetail_showsThrottledFlag() {
      counter.registered(100, 0);
      sensor.currentStatus();
      String detail = sensor.statusDetail();
      assertTrue(detail.contains("[throttled]"));
    }
  }

  @Nested
  class Concurrency {

    @Test
    void concurrentIncrementAndDecrement_countsAreCorrect() throws Exception {
      int threads = 8;
      int opsPerThread = 10_000;
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      CountDownLatch latch = new CountDownLatch(threads * 2);

      for (int t = 0; t < threads; t++) {
        executor.submit(
            () -> {
              for (int i = 0; i < opsPerThread; i++) {
                counter.registered(1, 0);
              }
              latch.countDown();
            });
        executor.submit(
            () -> {
              for (int i = 0; i < opsPerThread; i++) {
                counter.completed(1, 0);
              }
              latch.countDown();
            });
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
      executor.shutdown();

      assertEquals(0, counter.records(), "equal increments and decrements should net zero");
    }

    @Test
    void concurrentStatusChecks_doNotThrow() throws Exception {
      int threads = 4;
      int opsPerThread = 50_000;
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      CountDownLatch latch = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        executor.submit(
            () -> {
              for (int i = 0; i < opsPerThread; i++) {
                counter.registered(1, 0);
                sensor.currentStatus();
                counter.completed(1, 0);
                sensor.currentStatus();
              }
              latch.countDown();
            });
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
      executor.shutdown();
      assertEquals(0, counter.records());
    }
  }
}
