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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

/**
 * Backpressure sensor based on JVM heap usage.
 *
 * <p>Reads heap metrics via {@link MemoryMXBean} — reports {@code used/max} as a ratio (0.0–1.0).
 * Hysteresis prevents flapping: once THROTTLE is triggered at {@code throttleThreshold}, it stays
 * until usage drops below {@code resumeThreshold}.
 *
 * <p>{@code used} includes garbage the collector has not reclaimed yet, and with G1 or Parallel GC
 * it routinely climbs to 80–95% before a collection. Once the JVM has run a GC, the sensor
 * therefore uses the lower of {@code used} and the heap occupied right after the last collection
 * (the live set), so uncollected garbage alone does not throttle. Taking the lower value also means
 * resuming never depends on a GC happening while the consumer is paused.
 *
 * <p>This sensor protects against OOM regardless of pressure source — in-flight records, producer
 * buffers, caches, or any other heap-resident data.
 */
public final class HeapSensor implements BackpressureSensor {

  private final MemoryMXBean memoryMxBean;
  private final HeapBackpressureConfig config;
  // Heap used right after the last GC as a ratio of max, or NaN before the first GC.
  private final DoubleSupplier afterGcUsage;

  private volatile boolean throttled = false;

  /**
   * Creates a heap backpressure sensor using the default JVM memory bean.
   *
   * @param config the heap backpressure configuration
   */
  public HeapSensor(HeapBackpressureConfig config) {
    this(config, ManagementFactory.getMemoryMXBean(), new JvmAfterGcUsage());
  }

  /**
   * Creates a heap backpressure sensor with a custom memory bean (for testing).
   *
   * @param config the heap backpressure configuration
   * @param memoryMxBean the memory bean to read heap usage from
   */
  HeapSensor(HeapBackpressureConfig config, MemoryMXBean memoryMxBean) {
    this(config, memoryMxBean, () -> Double.NaN);
  }

  /**
   * Creates a heap backpressure sensor with custom heap readings (for testing).
   *
   * @param config the heap backpressure configuration
   * @param memoryMxBean the memory bean to read current heap usage from
   * @param afterGcUsage heap usage ratio right after the last GC, or NaN if unknown
   */
  HeapSensor(
      HeapBackpressureConfig config, MemoryMXBean memoryMxBean, DoubleSupplier afterGcUsage) {
    this.config = config;
    this.memoryMxBean = memoryMxBean;
    this.afterGcUsage = afterGcUsage;
  }

  /**
   * Reads the heap occupied right after the most recent GC (young or full), as a ratio of max heap,
   * from {@code com.sun.management.GcInfo}. That snapshot covers every heap pool at the end of the
   * same collection, unlike per-pool collection usage, which for the old generation under Serial or
   * Parallel GC only changes after a full GC. Recomputed only when a new GC has run. Returns NaN
   * before the first GC or when the JVM does not provide GC info, so the sensor uses current usage.
   */
  private static final class JvmAfterGcUsage implements DoubleSupplier {
    private long lastCollections = -1;
    private double cached = Double.NaN;
    private boolean unsupported;

    @Override
    public synchronized double getAsDouble() {
      if (unsupported) return Double.NaN;
      try {
        List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
        long collections = 0;
        for (GarbageCollectorMXBean gc : collectors) {
          collections += Math.max(0, gc.getCollectionCount());
        }
        if (collections == 0) return Double.NaN;
        if (collections == lastCollections) return cached;
        lastCollections = collections;

        Map<String, MemoryUsage> after = null;
        long latestEnd = Long.MIN_VALUE;
        boolean anyExtended = false;
        for (GarbageCollectorMXBean gc : collectors) {
          if (gc instanceof com.sun.management.GarbageCollectorMXBean ext) {
            anyExtended = true;
            com.sun.management.GcInfo info = ext.getLastGcInfo();
            if (info != null && info.getEndTime() >= latestEnd) {
              latestEnd = info.getEndTime();
              after = info.getMemoryUsageAfterGc();
            }
          }
        }
        if (!anyExtended) unsupported = true;
        if (after == null) return cached = Double.NaN;

        long used = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
          if (pool.getType() != MemoryType.HEAP) continue;
          MemoryUsage usage = after.get(pool.getName());
          if (usage != null) used += usage.getUsed();
        }
        long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        return cached = max > 0 ? (double) used / max : Double.NaN;
      } catch (LinkageError | RuntimeException e) {
        unsupported = true; // jdk.management not available: use current usage only
        return Double.NaN;
      }
    }
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
    String status = throttled ? " THROTTLED" : "";
    return String.format(
            "%.1f%% heap (threshold=%.0f%%)", usage * 100, config.throttleThreshold() * 100)
        + status;
  }

  private double heapUsage() {
    MemoryUsage heap = memoryMxBean.getHeapMemoryUsage();
    long max = heap.getMax();
    if (max <= 0) {
      return 0.0;
    }
    double current = (double) heap.getUsed() / max;
    double afterGc = afterGcUsage.getAsDouble();
    return Double.isNaN(afterGc) ? current : Math.min(current, afterGc);
  }
}
