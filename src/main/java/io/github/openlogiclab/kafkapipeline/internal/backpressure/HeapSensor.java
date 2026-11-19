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
import io.github.openlogiclab.kafkapipeline.backpressure.HeapBackpressureConfig;
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
 * Backpressure sensor on JVM heap usage, as a ratio of the maximum heap (0–1), with the same
 * high/low watermark hysteresis as the other built-in sensors ({@link WatermarkSensor}).
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
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

  private static final System.Logger logger = System.getLogger(HeapSensor.class.getName());

  private final MemoryMXBean memoryMxBean;
  // Heap used right after the last GC as a ratio of max, or NaN before the first GC.
  private final DoubleSupplier afterGcUsage;
  private final WatermarkSensor watermarks;

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
    this.memoryMxBean = memoryMxBean;
    this.afterGcUsage = afterGcUsage;
    this.watermarks =
        new WatermarkSensor(
            "heap",
            this::heapUsage,
            config.highWatermark(),
            config.lowWatermark(),
            v -> String.format("%.1f%%", v * 100));
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
        if (!anyExtended) markUnsupported("this JVM does not provide GC info");
        if (after == null) {
          lastCollections = collections;
          return cached = Double.NaN;
        }

        long used = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
          if (pool.getType() != MemoryType.HEAP) continue;
          MemoryUsage usage = after.get(pool.getName());
          if (usage != null) used += usage.getUsed();
        }
        long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        lastCollections = collections; // only now: a failed read is retried on the next call
        return cached = max > 0 ? (double) used / max : Double.NaN;
      } catch (LinkageError e) {
        // jdk.management is not in this runtime (for example a jlink image without it).
        markUnsupported(e.toString());
        return Double.NaN;
      } catch (RuntimeException e) {
        // A one-off failure reading GC info: NaN makes the sensor use current usage this time; the
        // read is tried again on the next call.
        logger.log(System.Logger.Level.DEBUG, "Could not read heap usage after GC", e);
        return Double.NaN;
      }
    }

    private void markUnsupported(String reason) {
      unsupported = true;
      logger.log(
          System.Logger.Level.INFO,
          "Heap backpressure uses current heap usage only, including uncollected garbage ({0})",
          reason);
    }
  }

  @Override
  public BackpressureStatus evaluate() {
    return watermarks.evaluate();
  }

  @Override
  public String name() {
    return watermarks.name();
  }

  @Override
  public String statusDetail() {
    return watermarks.statusDetail();
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
