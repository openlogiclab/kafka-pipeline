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
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BackpressureControllerTest {

  private static final BackpressureConfig CONFIG = new BackpressureConfig(100, 60, 500, true);

  @Nested
  class SingleSensor {

    @Test
    void ok_whenBelowThreshold() {
      InFlightCounter counter = new InFlightCounter();
      RecordCountSensor sensor = new RecordCountSensor(CONFIG, counter);
      BackpressureController controller = new BackpressureController(CONFIG, sensor);

      counter.registered(50, 0);
      assertEquals(BackpressureStatus.OK, controller.evaluate());
      assertFalse(controller.shouldThrottle());
    }

    @Test
    void throttle_whenAboveHighWatermark() {
      InFlightCounter counter = new InFlightCounter();
      RecordCountSensor sensor = new RecordCountSensor(CONFIG, counter);
      BackpressureController controller = new BackpressureController(CONFIG, sensor);

      counter.registered(100, 0);
      assertEquals(BackpressureStatus.THROTTLE, controller.evaluate());
      assertTrue(controller.shouldThrottle());
    }

    @Test
    void critical_whenAboveCritical() {
      InFlightCounter counter = new InFlightCounter();
      RecordCountSensor sensor = new RecordCountSensor(CONFIG, counter);
      BackpressureController controller = new BackpressureController(CONFIG, sensor);

      counter.registered(500, 0);
      assertEquals(BackpressureStatus.CRITICAL, controller.evaluate());
      assertTrue(controller.shouldThrottle());
    }
  }

  @Nested
  class MultipleSensors {

    @Test
    void worstStatusWins() {
      StubSensor okSensor = new StubSensor("ok-sensor", BackpressureStatus.OK);
      StubSensor throttleSensor = new StubSensor("throttle-sensor", BackpressureStatus.THROTTLE);

      BackpressureController controller =
          new BackpressureController(CONFIG, List.of(okSensor, throttleSensor));

      assertEquals(BackpressureStatus.THROTTLE, controller.evaluate());
    }

    @Test
    void critical_shortCircuits() {
      StubSensor criticalSensor = new StubSensor("critical", BackpressureStatus.CRITICAL);
      StubSensor okSensor = new StubSensor("ok", BackpressureStatus.OK);

      BackpressureController controller =
          new BackpressureController(CONFIG, List.of(criticalSensor, okSensor));

      assertEquals(BackpressureStatus.CRITICAL, controller.evaluate());
    }

    @Test
    void allOK_returnsOK() {
      StubSensor s1 = new StubSensor("s1", BackpressureStatus.OK);
      StubSensor s2 = new StubSensor("s2", BackpressureStatus.OK);

      BackpressureController controller = new BackpressureController(CONFIG, List.of(s1, s2));

      assertEquals(BackpressureStatus.OK, controller.evaluate());
    }
  }

  @Nested
  class Disabled {

    @Test
    void disabledController_alwaysOK() {
      StubSensor criticalSensor = new StubSensor("critical", BackpressureStatus.CRITICAL);
      BackpressureController controller =
          new BackpressureController(BackpressureConfig.disabled(), List.of(criticalSensor));

      assertEquals(BackpressureStatus.OK, controller.evaluate());
      assertFalse(controller.shouldThrottle());
    }

    @Test
    void isEnabled_reflectsConfig() {
      BackpressureController enabled = new BackpressureController(CONFIG, List.of());
      BackpressureController disabled =
          new BackpressureController(BackpressureConfig.disabled(), List.of());

      assertTrue(enabled.isEnabled());
      assertFalse(disabled.isEnabled());
    }
  }

  @Nested
  class StatusSummary {

    @Test
    void summary_includesSensorDetails() {
      InFlightCounter counter = new InFlightCounter();
      RecordCountSensor sensor = new RecordCountSensor(CONFIG, counter);
      counter.registered(42, 0);
      BackpressureController controller = new BackpressureController(CONFIG, sensor);

      String summary = controller.statusSummary();
      assertTrue(summary.contains("record-count"));
      assertTrue(summary.contains("42"));
    }

    @Test
    void summary_disabledController() {
      BackpressureController controller =
          new BackpressureController(BackpressureConfig.disabled(), List.of());

      assertEquals("backpressure=disabled", controller.statusSummary());
    }
  }

  @Nested
  class SensorsList {

    @Test
    void sensors_isImmutableCopy() {
      StubSensor s1 = new StubSensor("s1", BackpressureStatus.OK);
      BackpressureController controller = new BackpressureController(CONFIG, List.of(s1));

      assertThrows(
          UnsupportedOperationException.class,
          () -> controller.sensors().add(new StubSensor("s2", BackpressureStatus.OK)));
    }
  }

  @Nested
  class Integration {

    @Test
    void fullCycle_withRecordCountSensor() {
      InFlightCounter counter = new InFlightCounter();
      RecordCountSensor sensor = new RecordCountSensor(CONFIG, counter);
      BackpressureController controller = new BackpressureController(CONFIG, sensor);

      assertFalse(controller.shouldThrottle());

      counter.registered(100, 0);
      assertTrue(controller.shouldThrottle());

      counter.completed(41, 0);
      assertFalse(controller.shouldThrottle());

      counter.registered(50, 0);
      assertTrue(controller.shouldThrottle());

      counter.completed(49, 0);
      assertFalse(controller.shouldThrottle());
    }
  }

  private static class StubSensor implements BackpressureSensor {
    private final String name;
    private volatile BackpressureStatus status;

    StubSensor(String name, BackpressureStatus status) {
      this.name = name;
      this.status = status;
    }

    @Override
    public BackpressureStatus currentStatus() {
      return status;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String statusDetail() {
      return name + "=" + status;
    }
  }
}
