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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import java.util.List;

/**
 * Combines the pipeline's sensors into one global decision: throttle if any sensor says so.
 *
 * <p>The poll thread calls {@link #evaluate()} once per poll-loop iteration, which advances each
 * sensor's hysteresis. Other threads (metrics) read {@link #lastStatus()}, which never touches the
 * sensors.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class BackpressureController {

  private final List<BackpressureSensor> sensors;
  private volatile BackpressureStatus lastStatus = BackpressureStatus.OK;

  /**
   * Creates a controller. With no sensors it never throttles.
   *
   * @param sensors the sensors, all evaluated on every call
   */
  public BackpressureController(List<BackpressureSensor> sensors) {
    this.sensors = List.copyOf(sensors);
  }

  /**
   * Evaluates every sensor (so each keeps its hysteresis current) and returns the worst status.
   * Poll thread only.
   *
   * @return THROTTLE if any sensor throttles, otherwise OK
   */
  public BackpressureStatus evaluate() {
    BackpressureStatus worst = BackpressureStatus.OK;
    for (BackpressureSensor sensor : sensors) {
      if (sensor.evaluate() == BackpressureStatus.THROTTLE) {
        worst = BackpressureStatus.THROTTLE;
      }
    }
    lastStatus = worst;
    return worst;
  }

  /**
   * Returns the result of the last {@link #evaluate()}. Safe from any thread; no side effects.
   *
   * @return the last status
   */
  public BackpressureStatus lastStatus() {
    return lastStatus;
  }

  /**
   * Whether the poll loop should pause fetching; same as {@code evaluate() == THROTTLE}.
   *
   * @return true to pause every partition
   */
  public boolean shouldThrottle() {
    return evaluate() == BackpressureStatus.THROTTLE;
  }

  /**
   * Describes every sensor's reading, for logs.
   *
   * @return one line, sensors separated by {@code " | "}
   */
  public String statusSummary() {
    if (sensors.isEmpty()) return "no backpressure sensors";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < sensors.size(); i++) {
      if (i > 0) sb.append(" | ");
      sb.append(sensors.get(i).statusDetail());
    }
    return sb.toString();
  }

  /**
   * The sensors, in evaluation order.
   *
   * @return the sensors
   */
  public List<BackpressureSensor> sensors() {
    return sensors;
  }
}
