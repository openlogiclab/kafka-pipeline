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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HeapSensorTest {

  private static final long MAX_HEAP = 1000L;

  private static MemoryMXBean stubBean(long used) {
    return new StubMemoryMXBean(used, MAX_HEAP);
  }

  private HeapBackpressureConfig config() {
    return HeapBackpressureConfig.builder()
        .resumeThreshold(0.5)
        .throttleThreshold(0.7)
        .criticalThreshold(0.9)
        .build();
  }

  @Test
  void initialState_isOK() {
    HeapSensor sensor = new HeapSensor(config(), stubBean(0));
    assertEquals(BackpressureStatus.OK, sensor.currentStatus());
  }

  @Nested
  class HysteresisLogic {

    @Test
    void belowThrottle_staysOK() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(699));
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void atThrottle_triggersThrottle() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(700));
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void aboveThrottle_triggersThrottle() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(800));
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void atCritical_triggersCritical() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(900));
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());
    }

    @Test
    void aboveCritical_triggersCritical() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(950));
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());
    }

    @Test
    void throttled_staysThrottledAboveResume() {
      MutableStubMemoryMXBean bean = new MutableStubMemoryMXBean(800, MAX_HEAP);
      HeapSensor sensor = new HeapSensor(config(), bean);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      bean.setUsed(600);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void throttled_recoversAtResume() {
      MutableStubMemoryMXBean bean = new MutableStubMemoryMXBean(800, MAX_HEAP);
      HeapSensor sensor = new HeapSensor(config(), bean);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      bean.setUsed(500);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void throttled_recoversBelowResume() {
      MutableStubMemoryMXBean bean = new MutableStubMemoryMXBean(800, MAX_HEAP);
      HeapSensor sensor = new HeapSensor(config(), bean);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      bean.setUsed(200);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void critical_dropsToThrottle_aboveResume() {
      MutableStubMemoryMXBean bean = new MutableStubMemoryMXBean(950, MAX_HEAP);
      HeapSensor sensor = new HeapSensor(config(), bean);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());

      bean.setUsed(600);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void fullCycle_okThrottleCriticalThrottleOk() {
      MutableStubMemoryMXBean bean = new MutableStubMemoryMXBean(100, MAX_HEAP);
      HeapSensor sensor = new HeapSensor(config(), bean);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());

      bean.setUsed(700);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      bean.setUsed(900);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());

      bean.setUsed(600);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      bean.setUsed(500);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }
  }

  @Nested
  class DisabledConfig {

    @Test
    void disabled_alwaysReturnsOK() {
      HeapSensor sensor = new HeapSensor(HeapBackpressureConfig.disabled(), stubBean(999));
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }
  }

  @Nested
  class MaxHeapEdgeCases {

    @Test
    void maxHeapUndefined_returnsOK() {
      HeapSensor sensor = new HeapSensor(config(), new UndefinedMaxMemoryMXBean());
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }
  }

  @Nested
  class Metadata {

    @Test
    void name() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(0));
      assertEquals("heap", sensor.name());
    }

    @Test
    void statusDetail_containsPercentage() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(750));
      String detail = sensor.statusDetail();
      assertTrue(detail.contains("75.0%"), "Should contain usage percentage, got: " + detail);
      assertTrue(detail.contains("70%"), "Should contain threshold percentage, got: " + detail);
    }

    @Test
    void statusDetail_showsThrottledFlag() {
      HeapSensor sensor = new HeapSensor(config(), stubBean(800));
      sensor.currentStatus();
      String detail = sensor.statusDetail();
      assertTrue(detail.contains("[throttled]"), "Should show throttled flag, got: " + detail);
    }
  }

  private static class StubMemoryMXBean implements MemoryMXBean {
    private final long used;
    private final long max;

    StubMemoryMXBean(long used, long max) {
      this.used = used;
      this.max = max;
    }

    @Override
    public MemoryUsage getHeapMemoryUsage() {
      return new MemoryUsage(0, used, max > 0 ? max : 0, max);
    }

    @Override
    public MemoryUsage getNonHeapMemoryUsage() {
      return new MemoryUsage(0, 0, 0, 0);
    }

    @Override
    public int getObjectPendingFinalizationCount() {
      return 0;
    }

    @Override
    public boolean isVerbose() {
      return false;
    }

    @Override
    public void setVerbose(boolean value) {}

    @Override
    public void gc() {}

    @Override
    public javax.management.ObjectName getObjectName() {
      return null;
    }
  }

  private static class UndefinedMaxMemoryMXBean extends StubMemoryMXBean {
    UndefinedMaxMemoryMXBean() {
      super(500, 1000);
    }

    @Override
    public MemoryUsage getHeapMemoryUsage() {
      return new MemoryUsage(0, 500, 1000, -1);
    }
  }

  private static class MutableStubMemoryMXBean extends StubMemoryMXBean {
    private volatile long currentUsed;
    private final long maxHeap;

    MutableStubMemoryMXBean(long used, long max) {
      super(used, max);
      this.currentUsed = used;
      this.maxHeap = max;
    }

    void setUsed(long used) {
      this.currentUsed = used;
    }

    @Override
    public MemoryUsage getHeapMemoryUsage() {
      return new MemoryUsage(0, currentUsed, maxHeap, maxHeap);
    }
  }
}
