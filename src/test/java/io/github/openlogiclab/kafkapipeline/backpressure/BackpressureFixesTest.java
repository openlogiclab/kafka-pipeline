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

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class BackpressureFixesTest {

  private static MemoryMXBean heap(long used, long max) {
    return (MemoryMXBean)
        Proxy.newProxyInstance(
            MemoryMXBean.class.getClassLoader(),
            new Class<?>[] {MemoryMXBean.class},
            (proxy, method, args) ->
                method.getName().equals("getHeapMemoryUsage")
                    ? new MemoryUsage(0, used, max, max)
                    : null);
  }

  private static HeapBackpressureConfig config() {
    return HeapBackpressureConfig.builder()
        .resumeThreshold(0.5)
        .throttleThreshold(0.7)
        .criticalThreshold(0.9)
        .build();
  }

  @Test
  void uncollectedGarbage_doesNotThrottle() {
    // 95% used, but only 30% was live after the last GC.
    HeapSensor sensor = new HeapSensor(config(), heap(950, 1000), () -> 0.30);
    assertEquals(BackpressureStatus.OK, sensor.currentStatus());
  }

  @Test
  void liveSetAboveThreshold_throttles() {
    HeapSensor sensor = new HeapSensor(config(), heap(950, 1000), () -> 0.80);
    assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
  }

  @Test
  void resumeDoesNotWaitForAGc() {
    double[] afterGc = {0.80};
    long[] used = {950};
    HeapSensor sensor =
        new HeapSensor(
            config(),
            (MemoryMXBean)
                Proxy.newProxyInstance(
                    MemoryMXBean.class.getClassLoader(),
                    new Class<?>[] {MemoryMXBean.class},
                    (p, m, a) ->
                        m.getName().equals("getHeapMemoryUsage")
                            ? new MemoryUsage(0, used[0], 1000, 1000)
                            : null),
            () -> afterGc[0]);
    assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    used[0] = 400; // memory released; no GC has updated the after-GC reading yet
    assertEquals(BackpressureStatus.OK, sensor.currentStatus());
  }

  @Test
  void lastStatus_readsWithoutEvaluatingSensors() {
    int[] evaluations = {0};
    BackpressureSensor sensor =
        new BackpressureSensor() {
          @Override
          public BackpressureStatus currentStatus() {
            evaluations[0]++;
            return BackpressureStatus.THROTTLE;
          }

          @Override
          public String name() {
            return "counting";
          }

          @Override
          public String statusDetail() {
            return "";
          }
        };
    BackpressureController controller =
        new BackpressureController(BackpressureConfig.defaults(), sensor);
    assertEquals(BackpressureStatus.OK, controller.lastStatus());
    assertTrue(controller.shouldThrottle());
    assertEquals(BackpressureStatus.THROTTLE, controller.lastStatus());
    controller.lastStatus();
    assertEquals(1, evaluations[0], "lastStatus() must not advance sensor state");
  }

  @Test
  void realJvmReading_worksAfterAGc() {
    System.gc();
    HeapSensor sensor = new HeapSensor(config());
    BackpressureStatus status = sensor.currentStatus();
    assertNotNull(status);
    assertTrue(sensor.statusDetail().contains("heap"));
  }
}
