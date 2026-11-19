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
import java.util.Objects;
import java.util.function.DoubleFunction;
import java.util.function.DoubleSupplier;

/**
 * The one hysteresis rule all built-in sensors share: throttle when the reading reaches the high
 * watermark, and stay throttled until it has dropped to the low watermark. The gap keeps the
 * pipeline from pausing and resuming on every poll.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class WatermarkSensor implements BackpressureSensor {

  private final String name;
  private final DoubleSupplier reading;
  private final double high;
  private final double low;
  private final DoubleFunction<String> format;
  private volatile boolean throttled; // written by the poll thread only

  /**
   * Creates a sensor.
   *
   * @param name short name for logs
   * @param reading supplies the current reading; must be cheap
   * @param high reading at which to throttle
   * @param low reading at which to resume; less than {@code high}
   * @param format renders a reading for {@link #statusDetail()}, e.g. {@code v -> v + " records"}
   */
  public WatermarkSensor(
      String name, DoubleSupplier reading, double high, double low, DoubleFunction<String> format) {
    if (!(low < high)) {
      throw new IllegalArgumentException("low (" + low + ") must be < high (" + high + ")");
    }
    this.name = Objects.requireNonNull(name, "name");
    this.reading = Objects.requireNonNull(reading, "reading");
    this.high = high;
    this.low = low;
    this.format = Objects.requireNonNull(format, "format");
  }

  @Override
  public BackpressureStatus evaluate() {
    double current = reading.getAsDouble();
    if (throttled) {
      if (current <= low) throttled = false;
    } else if (current >= high) {
      throttled = true;
    }
    return throttled ? BackpressureStatus.THROTTLE : BackpressureStatus.OK;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String statusDetail() {
    return name
        + ": "
        + format.apply(reading.getAsDouble())
        + " (high="
        + format.apply(high)
        + ", low="
        + format.apply(low)
        + ")"
        + (throttled ? " THROTTLED" : "");
  }
}
