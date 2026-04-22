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

/**
 * Immutable configuration for heap-based backpressure thresholds.
 *
 * <p>Uses high/low watermark hysteresis (same pattern as {@link BackpressureConfig} and {@link
 * ByteBackpressureConfig}) but based on JVM heap usage ratio (0.0–1.0). This protects against OOM
 * when the application's overall memory usage grows — regardless of whether the pressure comes from
 * in-flight records, producer buffers, or other allocations.
 *
 * <p>Heap usage is measured via {@code MemoryMXBean.getHeapMemoryUsage()} which reports {@code
 * used/max} — the ratio of occupied heap to the JVM maximum ({@code -Xmx} or {@code
 * -XX:MaxRAMPercentage}).
 */
public record HeapBackpressureConfig(
    double throttleThreshold, double resumeThreshold, double criticalThreshold, boolean enabled) {

  public HeapBackpressureConfig {
    if (enabled) {
      if (resumeThreshold <= 0.0 || resumeThreshold >= 1.0) {
        throw new IllegalArgumentException(
            "resumeThreshold must be between 0.0 and 1.0 exclusive, got " + resumeThreshold);
      }
      if (throttleThreshold <= resumeThreshold) {
        throw new IllegalArgumentException(
            "throttleThreshold ("
                + throttleThreshold
                + ") must be > resumeThreshold ("
                + resumeThreshold
                + ")");
      }
      if (criticalThreshold <= throttleThreshold) {
        throw new IllegalArgumentException(
            "criticalThreshold ("
                + criticalThreshold
                + ") must be > throttleThreshold ("
                + throttleThreshold
                + ")");
      }
      if (criticalThreshold > 1.0) {
        throw new IllegalArgumentException(
            "criticalThreshold must be <= 1.0, got " + criticalThreshold);
      }
    }
  }

  public static HeapBackpressureConfig disabled() {
    return new HeapBackpressureConfig(0, 0, 0, false);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link HeapBackpressureConfig}. */
  public static final class Builder {
    private double throttleThreshold = 0.7;
    private double resumeThreshold = 0.5;
    private double criticalThreshold = 0.9;
    private boolean enabled = true;

    private Builder() {}

    /**
     * Heap usage ratio at which the consumer pauses polling. Must be greater than {@link
     * #resumeThreshold}.
     *
     * <p>Default: {@code 0.7} (70% heap usage).
     */
    public Builder throttleThreshold(double throttleThreshold) {
      this.throttleThreshold = throttleThreshold;
      return this;
    }

    /**
     * Heap usage ratio at which the consumer resumes polling after being paused. The gap between
     * throttle and resume thresholds prevents pause/resume flapping.
     *
     * <p>Default: {@code 0.5} (50% heap usage).
     */
    public Builder resumeThreshold(double resumeThreshold) {
      this.resumeThreshold = resumeThreshold;
      return this;
    }

    /**
     * Heap usage ratio at which status escalates to {@code CRITICAL}.
     *
     * <p>Default: {@code 0.9} (90% heap usage).
     */
    public Builder criticalThreshold(double criticalThreshold) {
      this.criticalThreshold = criticalThreshold;
      return this;
    }

    /**
     * Whether heap backpressure is enabled. When using {@link HeapBackpressureConfig#builder()},
     * defaults to {@code true}. Use {@link HeapBackpressureConfig#disabled()} to explicitly
     * disable.
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public HeapBackpressureConfig build() {
      return new HeapBackpressureConfig(
          throttleThreshold, resumeThreshold, criticalThreshold, enabled);
    }
  }
}
