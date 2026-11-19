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
 * Backpressure on JVM heap usage, to protect against running out of memory whatever uses it
 * (records in flight, producer buffers, caches).
 *
 * <p>Pauses fetching on every partition when heap usage reaches {@code highWatermark} and resumes
 * at {@code lowWatermark}. Both are ratios of the maximum heap ({@code -Xmx} or {@code
 * -XX:MaxRAMPercentage}) between 0 and 1. Usage is the lower of the current heap usage and the heap
 * occupied right after the most recent GC, so garbage the collector has not reclaimed yet does not
 * pause fetching, and resuming never has to wait for a GC. Off by default.
 *
 * @param highWatermark heap usage ratio at which fetching pauses, at most 1
 * @param lowWatermark heap usage ratio at which fetching resumes, above 0 and below {@code
 *     highWatermark}
 * @param enabled whether this sensor is active
 */
public record HeapBackpressureConfig(double highWatermark, double lowWatermark, boolean enabled) {

  /** Default high watermark: {@value}. */
  public static final double DEFAULT_HIGH_WATERMARK = 0.7;

  /** Default low watermark: {@value}. */
  public static final double DEFAULT_LOW_WATERMARK = 0.5;

  /**
   * Validates the watermarks when enabled.
   *
   * @param highWatermark heap usage ratio at which fetching pauses
   * @param lowWatermark heap usage ratio at which fetching resumes
   * @param enabled whether this sensor is active
   */
  public HeapBackpressureConfig {
    if (enabled) {
      if (Double.isNaN(highWatermark) || Double.isNaN(lowWatermark)) {
        throw new IllegalArgumentException("watermarks must be numbers, got NaN");
      }
      if (lowWatermark <= 0.0 || lowWatermark >= 1.0) {
        throw new IllegalArgumentException(
            "lowWatermark must be between 0 and 1 exclusive, got " + lowWatermark);
      }
      if (highWatermark <= lowWatermark || highWatermark > 1.0) {
        throw new IllegalArgumentException(
            "highWatermark must be > lowWatermark ("
                + lowWatermark
                + ") and <= 1, got "
                + highWatermark);
      }
    }
  }

  /**
   * Heap backpressure off (the default).
   *
   * @return a disabled configuration
   */
  public static HeapBackpressureConfig disabled() {
    return new HeapBackpressureConfig(0, 0, false);
  }

  /**
   * Returns a builder starting from {@value #DEFAULT_HIGH_WATERMARK} / {@value
   * #DEFAULT_LOW_WATERMARK}.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link HeapBackpressureConfig}. */
  public static final class Builder {
    private double highWatermark = DEFAULT_HIGH_WATERMARK;
    private double lowWatermark = DEFAULT_LOW_WATERMARK;

    private Builder() {}

    /**
     * Heap usage ratio at which fetching pauses. Default: {@value #DEFAULT_HIGH_WATERMARK}.
     *
     * @param highWatermark the high watermark (0–1)
     * @return this builder
     */
    public Builder highWatermark(double highWatermark) {
      this.highWatermark = highWatermark;
      return this;
    }

    /**
     * Heap usage ratio at which fetching resumes. Default: {@value #DEFAULT_LOW_WATERMARK}.
     *
     * @param lowWatermark the low watermark (0–1)
     * @return this builder
     */
    public Builder lowWatermark(double lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    /**
     * Builds an enabled configuration.
     *
     * @return the configuration
     */
    public HeapBackpressureConfig build() {
      return new HeapBackpressureConfig(highWatermark, lowWatermark, true);
    }
  }
}
