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

import java.util.List;

/**
 * Aggregates multiple {@link BackpressureSensor}s and returns the worst status.
 *
 * <p>The Poller calls {@link #evaluate()} once per poll cycle (not per record). Cost: one method
 * call per sensor — typically just an atomic read + int compare.
 *
 * <p>Thread-safe: sensors are immutable after construction; each sensor handles its own concurrency
 * internally.
 */
public final class BackpressureController {

  private final List<BackpressureSensor> sensors;
  private final boolean enabled;

  /**
   * Creates a controller with multiple sensors.
   *
   * @param config the backpressure configuration
   * @param sensors the sensors to aggregate
   */
  public BackpressureController(BackpressureConfig config, List<BackpressureSensor> sensors) {
    this.enabled = config.enabled();
    this.sensors = List.copyOf(sensors);
  }

  /**
   * Creates a controller with a single sensor.
   *
   * @param config the backpressure configuration
   * @param sensor the sensor to use
   */
  public BackpressureController(BackpressureConfig config, BackpressureSensor sensor) {
    this(config, List.of(sensor));
  }

  /**
   * Evaluates all sensors and returns the worst status.
   *
   * @return the worst status across all sensors
   */
  public BackpressureStatus evaluate() {
    if (!enabled) {
      return BackpressureStatus.OK;
    }

    BackpressureStatus worst = BackpressureStatus.OK;
    for (BackpressureSensor sensor : sensors) {
      BackpressureStatus status = sensor.currentStatus();
      if (status.ordinal() > worst.ordinal()) {
        worst = status;
        if (worst == BackpressureStatus.CRITICAL) {
          return worst;
        }
      }
    }
    return worst;
  }

  /**
   * Returns whether the poll loop should pause.
   *
   * @return true if any sensor indicates THROTTLE or CRITICAL
   */
  public boolean shouldThrottle() {
    return evaluate() != BackpressureStatus.OK;
  }

  /**
   * Returns a summary of all sensor statuses.
   *
   * @return human-readable status string
   */
  public String statusSummary() {
    if (!enabled) {
      return "backpressure disabled";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0, n = sensors.size(); i < n; i++) {
      if (i > 0) sb.append(" | ");
      sb.append(sensors.get(i).statusDetail());
    }
    return sb.toString();
  }

  /**
   * Returns whether backpressure is enabled.
   *
   * @return true if backpressure is active
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Returns the list of sensors being aggregated.
   *
   * @return immutable list of sensors
   */
  public List<BackpressureSensor> sensors() {
    return sensors;
  }
}
