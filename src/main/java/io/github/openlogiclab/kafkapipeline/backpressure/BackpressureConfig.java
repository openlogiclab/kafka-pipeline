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
 * Immutable configuration for backpressure thresholds.
 *
 * <p>Uses high/low watermark hysteresis to prevent rapid pause/resume flapping:
 *
 * <ul>
 *   <li>When in-flight count reaches {@code highWatermark} → THROTTLE
 *   <li>Stays THROTTLE until count drops to {@code lowWatermark} → OK
 *   <li>If count reaches {@code criticalThreshold} → CRITICAL
 * </ul>
 *
 * <p>Use {@link #builder()} for custom values, or {@link #defaults()} for sensible defaults.
 */
public record BackpressureConfig(
    int highWatermark, int lowWatermark, int criticalThreshold, boolean enabled) {
  public BackpressureConfig {
    if (enabled) {
      if (lowWatermark <= 0) {
        throw new IllegalArgumentException("lowWatermark must be positive, got " + lowWatermark);
      }
      if (highWatermark <= lowWatermark) {
        throw new IllegalArgumentException(
            "highWatermark (" + highWatermark + ") must be > lowWatermark (" + lowWatermark + ")");
      }
      if (criticalThreshold <= highWatermark) {
        throw new IllegalArgumentException(
            "criticalThreshold ("
                + criticalThreshold
                + ") must be > highWatermark ("
                + highWatermark
                + ")");
      }
    }
  }

  public static BackpressureConfig defaults() {
    return new BackpressureConfig(10_000, 6_000, 50_000, true);
  }

  public static BackpressureConfig disabled() {
    return new BackpressureConfig(0, 0, 0, false);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link BackpressureConfig}. */
  public static final class Builder {
    private int highWatermark = 10_000;
    private int lowWatermark = 6_000;
    private int criticalThreshold = 50_000;
    private boolean enabled = true;

    private Builder() {}

    /**
     * In-flight record count at which the consumer pauses polling. Must be greater than {@link
     * #lowWatermark}.
     *
     * <p>Default: {@code 10,000}.
     */
    public Builder highWatermark(int highWatermark) {
      this.highWatermark = highWatermark;
      return this;
    }

    /**
     * In-flight record count at which the consumer resumes polling after being paused. The gap
     * between high and low watermarks is the hysteresis band that prevents pause/resume flapping.
     *
     * <p>Default: {@code 6,000}.
     */
    public Builder lowWatermark(int lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    /**
     * Hard ceiling for in-flight records. When reached, the status escalates to {@code CRITICAL}.
     * In practice this should be set much higher than {@code highWatermark} as a last-resort safety
     * net.
     *
     * <p>Default: {@code 50,000}.
     */
    public Builder criticalThreshold(int criticalThreshold) {
      this.criticalThreshold = criticalThreshold;
      return this;
    }

    /**
     * Whether record-count backpressure is enabled. Defaults to {@code true}. Disabling is not
     * recommended for concurrent pipelines.
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public BackpressureConfig build() {
      return new BackpressureConfig(highWatermark, lowWatermark, criticalThreshold, enabled);
    }
  }
}
