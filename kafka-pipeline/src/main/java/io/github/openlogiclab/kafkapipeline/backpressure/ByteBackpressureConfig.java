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
 * Backpressure on the bytes of records in flight, to protect memory when records are large.
 *
 * <p>Pauses fetching on every partition when the in-flight bytes reach {@code highWatermarkBytes}
 * and resumes at {@code lowWatermarkBytes}. Sizes are the serialized key and value sizes ({@code
 * ConsumerRecord.serializedKeySize() + serializedValueSize()}), the size on the wire. Deserialized
 * objects are often 2–10× larger on the heap (JSON trees, Avro records, UTF-16 strings), so set the
 * watermarks with that factor in mind, or combine this with {@link HeapBackpressureConfig}. Off by
 * default.
 *
 * @param highWatermarkBytes in-flight bytes at which fetching pauses
 * @param lowWatermarkBytes in-flight bytes at which fetching resumes; less than {@code
 *     highWatermarkBytes}
 * @param enabled whether this sensor is active
 */
public record ByteBackpressureConfig(
    long highWatermarkBytes, long lowWatermarkBytes, boolean enabled) {

  /** Default high watermark: 256 MiB. */
  public static final long DEFAULT_HIGH_WATERMARK_BYTES = 256L * 1024 * 1024;

  /** Default low watermark: 128 MiB. */
  public static final long DEFAULT_LOW_WATERMARK_BYTES = 128L * 1024 * 1024;

  /**
   * Validates the watermarks when enabled.
   *
   * @param highWatermarkBytes in-flight bytes at which fetching pauses
   * @param lowWatermarkBytes in-flight bytes at which fetching resumes
   * @param enabled whether this sensor is active
   */
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
    }
  }

  /**
   * Byte backpressure off (the default).
   *
   * @return a disabled configuration
   */
  public static ByteBackpressureConfig disabled() {
    return new ByteBackpressureConfig(0, 0, false);
  }

  /**
   * Returns a builder starting from 256 MiB / 128 MiB.
   *
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ByteBackpressureConfig}. */
  public static final class Builder {
    private long highWatermarkBytes = DEFAULT_HIGH_WATERMARK_BYTES;
    private long lowWatermarkBytes = DEFAULT_LOW_WATERMARK_BYTES;

    private Builder() {}

    /**
     * In-flight bytes at which fetching pauses. Default: 256 MiB.
     *
     * @param highWatermarkBytes the high watermark
     * @return this builder
     */
    public Builder highWatermarkBytes(long highWatermarkBytes) {
      this.highWatermarkBytes = highWatermarkBytes;
      return this;
    }

    /**
     * In-flight bytes at which fetching resumes. Default: 128 MiB.
     *
     * @param lowWatermarkBytes the low watermark
     * @return this builder
     */
    public Builder lowWatermarkBytes(long lowWatermarkBytes) {
      this.lowWatermarkBytes = lowWatermarkBytes;
      return this;
    }

    /**
     * Builds an enabled configuration.
     *
     * @return the configuration
     */
    public ByteBackpressureConfig build() {
      return new ByteBackpressureConfig(highWatermarkBytes, lowWatermarkBytes, true);
    }
  }
}
