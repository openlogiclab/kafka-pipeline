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
 * Backpressure sensor based on in-flight payload bytes.
 *
 * <p>Reads from {@link InFlightCounter#bytes()} — never writes. Hysteresis prevents flapping: once
 * THROTTLE is triggered at {@code highWatermarkBytes}, it stays until bytes drop below {@code
 * lowWatermarkBytes}.
 */
public final class ByteSizeSensor implements BackpressureSensor {

  private final InFlightCounter counter;
  private final ByteBackpressureConfig config;

  private volatile boolean throttled = false;

  public ByteSizeSensor(ByteBackpressureConfig config, InFlightCounter counter) {
    this.config = config;
    this.counter = counter;
  }

  @Override
  public BackpressureStatus currentStatus() {
    if (!config.enabled()) {
      return BackpressureStatus.OK;
    }

    long current = counter.bytes();

    if (current >= config.criticalThresholdBytes()) {
      throttled = true;
      return BackpressureStatus.CRITICAL;
    }

    if (throttled) {
      if (current <= config.lowWatermarkBytes()) {
        throttled = false;
        return BackpressureStatus.OK;
      }
      return BackpressureStatus.THROTTLE;
    }

    if (current >= config.highWatermarkBytes()) {
      throttled = true;
      return BackpressureStatus.THROTTLE;
    }

    return BackpressureStatus.OK;
  }

  @Override
  public String name() {
    return "byte-size";
  }

  @Override
  public String statusDetail() {
    long current = counter.bytes();
    return formatBytes(current)
        + "/"
        + formatBytes(config.highWatermarkBytes())
        + " in-flight"
        + (throttled ? " [throttled]" : "");
  }

  private static String formatBytes(long bytes) {
    if (bytes >= 1024L * 1024 * 1024) {
      return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    } else if (bytes >= 1024L * 1024) {
      return String.format("%.1fMB", bytes / (1024.0 * 1024));
    } else if (bytes >= 1024) {
      return String.format("%.1fKB", bytes / 1024.0);
    }
    return bytes + "B";
  }
}
