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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ByteSizeSensorTest {

  private InFlightCounter counter;
  private ByteSizeSensor sensor;

  @BeforeEach
  void setUp() {
    ByteBackpressureConfig config =
        ByteBackpressureConfig.builder()
            .lowWatermarkBytes(100)
            .highWatermarkBytes(200)
            .criticalThresholdBytes(400)
            .build();
    counter = new InFlightCounter();
    sensor = new ByteSizeSensor(config, counter);
  }

  @Test
  void initialState_isOK() {
    assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    assertEquals(0, counter.bytes());
  }

  @Test
  void registerAndComplete_tracksBytes() {
    counter.registered(0, 50);
    assertEquals(50, counter.bytes());

    counter.completed(0, 30);
    assertEquals(20, counter.bytes());
  }

  @Nested
  class HysteresisLogic {

    @Test
    void belowHighWatermark_staysOK() {
      counter.registered(0, 199);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void atHighWatermark_triggersThrottle() {
      counter.registered(0, 200);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void aboveHighWatermark_triggersThrottle() {
      counter.registered(0, 250);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void atCriticalThreshold_triggersCritical() {
      counter.registered(0, 400);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());
    }

    @Test
    void throttled_staysThrottledAboveLowWatermark() {
      counter.registered(0, 250);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(0, 100);
      assertEquals(150, counter.bytes());
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void throttled_recoversAtLowWatermark() {
      counter.registered(0, 250);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(0, 150);
      assertEquals(100, counter.bytes());
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void throttled_recoversbelowLowWatermark() {
      counter.registered(0, 250);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(0, 200);
      assertEquals(50, counter.bytes());
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }

    @Test
    void critical_dropsToThrottle_aboveLow() {
      counter.registered(0, 500);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());

      counter.completed(0, 350);
      assertEquals(150, counter.bytes());
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());
    }

    @Test
    void fullCycle_okThrottleCriticalThrottleOk() {
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());

      counter.registered(0, 200);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.registered(0, 200);
      assertEquals(BackpressureStatus.CRITICAL, sensor.currentStatus());

      counter.completed(0, 250);
      assertEquals(BackpressureStatus.THROTTLE, sensor.currentStatus());

      counter.completed(0, 100);
      assertEquals(BackpressureStatus.OK, sensor.currentStatus());
    }
  }

  @Nested
  class DisabledConfig {

    @Test
    void disabled_alwaysReturnsOK() {
      InFlightCounter c = new InFlightCounter();
      ByteSizeSensor disabled = new ByteSizeSensor(ByteBackpressureConfig.disabled(), c);
      c.registered(0, 999_999_999);
      assertEquals(BackpressureStatus.OK, disabled.currentStatus());
    }
  }

  @Nested
  class EstimateBytes {

    @Test
    void estimateBytes_returnsNonNegative() {
      ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
      long bytes = io.github.openlogiclab.kafkapipeline.RecordSize.estimateBytes(record);
      assertTrue(bytes >= 0, "Estimated bytes should be non-negative, got " + bytes);
    }

    @Test
    void estimateBytes_handlesNullKey() {
      ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, null, "value");
      long bytes = io.github.openlogiclab.kafkapipeline.RecordSize.estimateBytes(record);
      assertTrue(bytes >= 0, "Estimated bytes should be non-negative");
    }

    @Test
    void estimateBytes_handlesNullValue() {
      ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", null);
      long bytes = io.github.openlogiclab.kafkapipeline.RecordSize.estimateBytes(record);
      assertTrue(bytes >= 0, "Estimated bytes should be non-negative");
    }
  }

  @Nested
  class Metadata {

    @Test
    void name() {
      assertEquals("byte-size", sensor.name());
    }

    @Test
    void statusDetail_containsFormattedBytes() {
      counter.registered(0, 1024 * 1024);
      String detail = sensor.statusDetail();
      assertTrue(detail.contains("MB"), "Should format bytes as MB");
      assertTrue(detail.contains("in-flight"), "Should contain in-flight label");
    }

    @Test
    void statusDetail_showsThrottledFlag() {
      counter.registered(0, 300);
      sensor.currentStatus();
      String detail = sensor.statusDetail();
      assertTrue(detail.contains("[throttled]"), "Should show throttled flag");
    }
  }
}
