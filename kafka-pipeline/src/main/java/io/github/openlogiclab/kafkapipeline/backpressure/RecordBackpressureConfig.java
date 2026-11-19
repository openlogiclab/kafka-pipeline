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
 * Backpressure on the number of records in flight: polled and not yet finished (including records
 * whose sink write is still pending).
 *
 * <p>When the count reaches {@code highWatermark}, the pipeline pauses fetching on every partition;
 * it resumes once the count has dropped to {@code lowWatermark}. The gap between the two keeps it
 * from pausing and resuming on every poll. On by default.
 *
 * @param highWatermark in-flight records at which fetching pauses
 * @param lowWatermark in-flight records at which fetching resumes; less than {@code highWatermark}
 * @param enabled whether this sensor is active
 */
public record RecordBackpressureConfig(int highWatermark, int lowWatermark, boolean enabled) {

  /** Default high watermark: {@value}. */
  public static final int DEFAULT_HIGH_WATERMARK = 10_000;

  /** Default low watermark: {@value}. */
  public static final int DEFAULT_LOW_WATERMARK = 6_000;

  /**
   * Validates the watermarks when enabled.
   *
   * @param highWatermark in-flight records at which fetching pauses
   * @param lowWatermark in-flight records at which fetching resumes
   * @param enabled whether this sensor is active
   */
  public RecordBackpressureConfig {
    if (enabled) {
      if (lowWatermark <= 0) {
        throw new IllegalArgumentException("lowWatermark must be positive, got " + lowWatermark);
      }
      if (highWatermark <= lowWatermark) {
        throw new IllegalArgumentException(
            "highWatermark (" + highWatermark + ") must be > lowWatermark (" + lowWatermark + ")");
      }
    }
  }

  /**
   * The default: on, pause at {@value #DEFAULT_HIGH_WATERMARK}, resume at {@value
   * #DEFAULT_LOW_WATERMARK}.
   *
   * @return the default configuration
   */
  public static RecordBackpressureConfig defaults() {
    return new RecordBackpressureConfig(DEFAULT_HIGH_WATERMARK, DEFAULT_LOW_WATERMARK, true);
  }

  /**
   * Turns record-count backpressure off. Other sensors (bytes, heap, sink, custom) still apply.
   *
   * @return a disabled configuration
   */
  public static RecordBackpressureConfig disabled() {
    return new RecordBackpressureConfig(0, 0, false);
  }

  /**
   * Returns a builder starting from the defaults.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link RecordBackpressureConfig}. */
  public static final class Builder {
    private int highWatermark = DEFAULT_HIGH_WATERMARK;
    private int lowWatermark = DEFAULT_LOW_WATERMARK;

    private Builder() {}

    /**
     * In-flight records at which fetching pauses. Default: {@value #DEFAULT_HIGH_WATERMARK}.
     *
     * @param highWatermark the high watermark
     * @return this builder
     */
    public Builder highWatermark(int highWatermark) {
      this.highWatermark = highWatermark;
      return this;
    }

    /**
     * In-flight records at which fetching resumes. Default: {@value #DEFAULT_LOW_WATERMARK}.
     *
     * @param lowWatermark the low watermark
     * @return this builder
     */
    public Builder lowWatermark(int lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    /**
     * Builds an enabled configuration.
     *
     * @return the configuration
     */
    public RecordBackpressureConfig build() {
      return new RecordBackpressureConfig(highWatermark, lowWatermark, true);
    }
  }
}
