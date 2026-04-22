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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * Backpressure sensor based on JVM heap usage.
 *
 * <p>Reads heap metrics via {@link MemoryMXBean} — reports {@code used/max} as a ratio (0.0–1.0).
 * Hysteresis prevents flapping: once THROTTLE is triggered at {@code throttleThreshold}, it stays
 * until usage drops below {@code resumeThreshold}.
 *
 * <p>This sensor protects against OOM regardless of pressure source — in-flight records, producer
 * buffers, caches, or any other heap-resident data.
 */
public final class HeapSensor implements BackpressureSensor {

  private final MemoryMXBean memoryMxBean;
  private final HeapBackpressureConfig config;

  private volatile boolean throttled = false;

  public HeapSensor(HeapBackpressureConfig config) {
    this(config, ManagementFactory.getMemoryMXBean());
  }

  HeapSensor(HeapBackpressureConfig config, MemoryMXBean memoryMxBean) {
    this.config = config;
    this.memoryMxBean = memoryMxBean;
  }

  @Override
  public BackpressureStatus currentStatus() {
    if (!config.enabled()) {
      return BackpressureStatus.OK;
    }

    double usage = heapUsage();

    if (usage >= config.criticalThreshold()) {
      throttled = true;
      return BackpressureStatus.CRITICAL;
    }

    if (throttled) {
      if (usage <= config.resumeThreshold()) {
        throttled = false;
        return BackpressureStatus.OK;
      }
      return BackpressureStatus.THROTTLE;
    }

    if (usage >= config.throttleThreshold()) {
      throttled = true;
      return BackpressureStatus.THROTTLE;
    }

    return BackpressureStatus.OK;
  }

  @Override
  public String name() {
    return "heap";
  }

  @Override
  public String statusDetail() {
    double usage = heapUsage();
    return String.format("%.1f%%/%.0f%% heap", usage * 100, config.throttleThreshold() * 100)
        + (throttled ? " [throttled]" : "");
  }

  private double heapUsage() {
    MemoryUsage heap = memoryMxBean.getHeapMemoryUsage();
    long max = heap.getMax();
    if (max <= 0) {
      return 0.0;
    }
    return (double) heap.getUsed() / max;
  }
}
