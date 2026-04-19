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

  public BackpressureController(BackpressureConfig config, List<BackpressureSensor> sensors) {
    this.enabled = config.enabled();
    this.sensors = List.copyOf(sensors);
  }

  public BackpressureController(BackpressureConfig config, BackpressureSensor sensor) {
    this(config, List.of(sensor));
  }

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

  public boolean shouldThrottle() {
    return evaluate() != BackpressureStatus.OK;
  }

  public String statusSummary() {
    if (!enabled) {
      return "backpressure=disabled";
    }
    StringBuilder sb = new StringBuilder("backpressure{");
    for (int i = 0, n = sensors.size(); i < n; i++) {
      if (i > 0) sb.append(", ");
      BackpressureSensor s = sensors.get(i);
      sb.append(s.name()).append('=').append(s.statusDetail());
    }
    return sb.append('}').toString();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public List<BackpressureSensor> sensors() {
    return sensors;
  }
}
