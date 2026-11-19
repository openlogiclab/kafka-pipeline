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
package io.github.openlogiclab.kafkapipeline.internal.backpressure;

import static io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus.OK;
import static io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus.THROTTLE;
import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.backpressure.ByteBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.HeapBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.RecordBackpressureConfig;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BuiltInSensorsTest {

  @Nested
  class Watermarks {

    @Test
    void throttlesAtHighAndResumesOnlyAtLow() {
      AtomicLong value = new AtomicLong();
      WatermarkSensor sensor = new WatermarkSensor("test", value::get, 10, 5, v -> (long) v + "");
      assertEquals(OK, sensor.evaluate());
      value.set(9);
      assertEquals(OK, sensor.evaluate());
      value.set(10);
      assertEquals(THROTTLE, sensor.evaluate());
      value.set(6);
      assertEquals(THROTTLE, sensor.evaluate(), "hysteresis: still above the low watermark");
      value.set(5);
      assertEquals(OK, sensor.evaluate());
      value.set(9);
      assertEquals(OK, sensor.evaluate(), "below high again: not throttled");
      assertTrue(sensor.statusDetail().contains("test: 9 (high=10, low=5)"));
    }

    @Test
    void lowMustBeBelowHigh() {
      assertThrows(
          IllegalArgumentException.class, () -> new WatermarkSensor("x", () -> 0, 5, 5, v -> ""));
    }

    @Test
    void recordAndByteSensorsReadTheInFlightCounter() {
      InFlightCounter counter = new InFlightCounter();
      BackpressureSensor records =
          BuiltInSensors.records(
              RecordBackpressureConfig.builder().highWatermark(3).lowWatermark(1).build(), counter);
      BackpressureSensor bytes =
          BuiltInSensors.bytes(
              ByteBackpressureConfig.builder()
                  .highWatermarkBytes(1_000)
                  .lowWatermarkBytes(100)
                  .build(),
              counter);
      counter.registered(3, 500);
      assertEquals(THROTTLE, records.evaluate());
      assertEquals(OK, bytes.evaluate());
      counter.registered(0, 600);
      assertEquals(THROTTLE, bytes.evaluate());
      counter.completed(3, 1_100);
      assertEquals(OK, records.evaluate());
      assertEquals(OK, bytes.evaluate());
      assertEquals("record-count", records.name());
    }

    @Test
    void watermarkFactory_validatesAndThrottles() {
      AtomicLong pending = new AtomicLong(4);
      BackpressureSensor sensor = BackpressureSensor.watermark("sends", pending::get, 4, 2);
      assertEquals(THROTTLE, sensor.evaluate());
      pending.set(3);
      assertEquals(THROTTLE, sensor.evaluate(), "still above the low watermark");
      pending.set(2);
      assertEquals(OK, sensor.evaluate());
      assertEquals("sends", sensor.name());
      assertTrue(sensor.statusDetail().startsWith("sends: 2 pending"));
      assertThrows(
          IllegalArgumentException.class, () -> BackpressureSensor.watermark("x", () -> 0, 2, 2));
      assertThrows(
          IllegalArgumentException.class, () -> BackpressureSensor.watermark("x", () -> 0, 0, 0));
      assertThrows(
          IllegalArgumentException.class, () -> BackpressureSensor.watermark("x", () -> 0, 5, -1));
      assertThrows(
          NullPointerException.class, () -> BackpressureSensor.watermark(null, () -> 0, 5, 1));
      assertThrows(NullPointerException.class, () -> BackpressureSensor.watermark("x", null, 5, 1));
    }
  }

  @Nested
  class Heap {

    private MemoryMXBean heap(long[] used) {
      return (MemoryMXBean)
          Proxy.newProxyInstance(
              MemoryMXBean.class.getClassLoader(),
              new Class<?>[] {MemoryMXBean.class},
              (p, m, a) ->
                  m.getName().equals("getHeapMemoryUsage")
                      ? new MemoryUsage(0, used[0], 1000, 1000)
                      : null);
    }

    private final HeapBackpressureConfig config =
        HeapBackpressureConfig.builder().highWatermark(0.7).lowWatermark(0.5).build();

    @Test
    void throttlesOnCurrentUsageBeforeAnyGc() {
      long[] used = {800};
      HeapSensor sensor = new HeapSensor(config, heap(used));
      assertEquals(THROTTLE, sensor.evaluate());
      used[0] = 500;
      assertEquals(OK, sensor.evaluate());
    }

    @Test
    void uncollectedGarbageDoesNotThrottle() {
      HeapSensor sensor = new HeapSensor(config, heap(new long[] {950}), () -> 0.30);
      assertEquals(OK, sensor.evaluate());
    }

    @Test
    void resumingDoesNotWaitForAGc() {
      long[] used = {950};
      HeapSensor sensor = new HeapSensor(config, heap(used), () -> 0.80);
      assertEquals(THROTTLE, sensor.evaluate());
      used[0] = 400; // released; the after-GC reading is stale
      assertEquals(OK, sensor.evaluate());
    }

    @Test
    void realJvmReadingWorks() {
      System.gc();
      HeapSensor sensor = new HeapSensor(config);
      assertNotNull(sensor.evaluate());
      assertTrue(sensor.statusDetail().startsWith("heap: "));
    }
  }

  @Nested
  class Controller {

    private static BackpressureSensor fixed(BackpressureStatus status, int[] calls) {
      return new BackpressureSensor() {
        @Override
        public BackpressureStatus evaluate() {
          calls[0]++;
          return status;
        }

        @Override
        public String name() {
          return status.name();
        }

        @Override
        public String statusDetail() {
          return status.name();
        }
      };
    }

    @Test
    void throttlesIfAnySensorThrottles_andEvaluatesEverySensor() {
      int[] okCalls = {0};
      int[] throttleCalls = {0};
      BackpressureController controller =
          new BackpressureController(List.of(fixed(THROTTLE, throttleCalls), fixed(OK, okCalls)));
      assertEquals(OK, controller.lastStatus(), "nothing evaluated yet");
      assertTrue(controller.shouldThrottle());
      assertEquals(1, okCalls[0], "later sensors still advance their hysteresis");
      assertEquals(THROTTLE, controller.lastStatus());
      controller.lastStatus();
      assertEquals(1, throttleCalls[0], "lastStatus() never evaluates");
      assertEquals("THROTTLE | OK", controller.statusSummary());
    }

    @Test
    void noSensors_neverThrottles() {
      BackpressureController controller = new BackpressureController(List.of());
      assertFalse(controller.shouldThrottle());
      assertEquals("no backpressure sensors", controller.statusSummary());
    }
  }
}
