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

import io.github.openlogiclab.kafkapipeline.InFlightCounter;

/**
 * Backpressure sensor based on in-flight record count.
 *
 * <p>Reads from {@link InFlightCounter#records()} — never writes. Hysteresis prevents flapping:
 * once THROTTLE is triggered at {@code highWatermark}, it stays until count drops below {@code
 * lowWatermark}.
 */
public final class RecordCountSensor implements BackpressureSensor {

  private final InFlightCounter counter;
  private final BackpressureConfig config;

  private volatile boolean throttled = false;

  public RecordCountSensor(BackpressureConfig config, InFlightCounter counter) {
    this.config = config;
    this.counter = counter;
  }

  @Override
  public BackpressureStatus currentStatus() {
    if (!config.enabled()) {
      return BackpressureStatus.OK;
    }

    int current = counter.records();

    if (current >= config.criticalThreshold()) {
      throttled = true;
      return BackpressureStatus.CRITICAL;
    }

    if (throttled) {
      if (current <= config.lowWatermark()) {
        throttled = false;
        return BackpressureStatus.OK;
      }
      return BackpressureStatus.THROTTLE;
    }

    if (current >= config.highWatermark()) {
      throttled = true;
      return BackpressureStatus.THROTTLE;
    }

    return BackpressureStatus.OK;
  }

  @Override
  public String name() {
    return "record-count";
  }

  @Override
  public String statusDetail() {
    int current = counter.records();
    return current
        + "/"
        + config.highWatermark()
        + " in-flight"
        + (throttled ? " [throttled]" : "");
  }
}
