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
 * Immutable configuration for byte-level backpressure thresholds.
 *
 * <p>Uses high/low watermark hysteresis (same pattern as {@link BackpressureConfig}) but based on
 * total in-flight payload bytes instead of record count. This protects against memory pressure when
 * individual records are large (e.g. 10 MB file payloads).
 *
 * <p>Size estimation uses {@code ConsumerRecord.serializedKeySize() +
 * ConsumerRecord.serializedValueSize()} — the wire payload size, not Java heap overhead.
 */
public record ByteBackpressureConfig(
    long highWatermarkBytes, long lowWatermarkBytes, long criticalThresholdBytes, boolean enabled) {

  public ByteBackpressureConfig {
    if (enabled) {
      if (lowWatermarkBytes <= 0) {
        throw new IllegalArgumentException(
            "lowWatermarkBytes must be positive, got " + lowWatermarkBytes);
      }
      if (highWatermarkBytes <= lowWatermarkBytes) {
        throw new IllegalArgumentException(
            "highWatermarkBytes ("
                + highWatermarkBytes
                + ") must be > lowWatermarkBytes ("
                + lowWatermarkBytes
                + ")");
      }
      if (criticalThresholdBytes <= highWatermarkBytes) {
        throw new IllegalArgumentException(
            "criticalThresholdBytes ("
                + criticalThresholdBytes
                + ") must be > highWatermarkBytes ("
                + highWatermarkBytes
                + ")");
      }
    }
  }

  public static ByteBackpressureConfig disabled() {
    return new ByteBackpressureConfig(0, 0, 0, false);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ByteBackpressureConfig}. */
  public static final class Builder {
    private long highWatermarkBytes = 256L * 1024 * 1024;
    private long lowWatermarkBytes = 128L * 1024 * 1024;
    private long criticalThresholdBytes = 512L * 1024 * 1024;
    private boolean enabled = true;

    private Builder() {}

    /**
     * Total in-flight bytes at which the consumer pauses polling. Must be greater than {@link
     * #lowWatermarkBytes}.
     *
     * <p>Default: {@code 256 MB}.
     */
    public Builder highWatermarkBytes(long highWatermarkBytes) {
      this.highWatermarkBytes = highWatermarkBytes;
      return this;
    }

    /**
     * Total in-flight bytes at which the consumer resumes polling after being paused. The gap
     * between high and low watermarks prevents pause/resume flapping.
     *
     * <p>Default: {@code 128 MB}.
     */
    public Builder lowWatermarkBytes(long lowWatermarkBytes) {
      this.lowWatermarkBytes = lowWatermarkBytes;
      return this;
    }

    /**
     * Hard ceiling for total in-flight bytes. When reached, status escalates to {@code CRITICAL}.
     *
     * <p>Default: {@code 512 MB}.
     */
    public Builder criticalThresholdBytes(long criticalThresholdBytes) {
      this.criticalThresholdBytes = criticalThresholdBytes;
      return this;
    }

    /**
     * Whether byte-level backpressure is enabled. When using {@link
     * ByteBackpressureConfig#builder()}, defaults to {@code true}. Use {@link
     * ByteBackpressureConfig#disabled()} to explicitly disable.
     */
    public Builder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public ByteBackpressureConfig build() {
      return new ByteBackpressureConfig(
          highWatermarkBytes, lowWatermarkBytes, criticalThresholdBytes, enabled);
    }
  }
}
