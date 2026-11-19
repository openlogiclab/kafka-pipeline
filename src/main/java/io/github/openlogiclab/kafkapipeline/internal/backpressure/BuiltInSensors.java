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
import io.github.openlogiclab.kafkapipeline.backpressure.ByteBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.HeapBackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.RecordBackpressureConfig;

/**
 * Builds the sensors behind the pipeline's backpressure settings.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class BuiltInSensors {

  private BuiltInSensors() {}

  /**
   * Throttles on the number of records in flight.
   *
   * @param config the watermarks
   * @param counter the in-flight counter
   * @return the sensor
   */
  public static BackpressureSensor records(
      RecordBackpressureConfig config, InFlightCounter counter) {
    return new WatermarkSensor(
        "record-count",
        counter::records,
        config.highWatermark(),
        config.lowWatermark(),
        v -> (long) v + " records");
  }

  /**
   * Throttles on the serialized bytes of records in flight.
   *
   * @param config the watermarks
   * @param counter the in-flight counter
   * @return the sensor
   */
  public static BackpressureSensor bytes(ByteBackpressureConfig config, InFlightCounter counter) {
    return new WatermarkSensor(
        "byte-size",
        counter::bytes,
        config.highWatermarkBytes(),
        config.lowWatermarkBytes(),
        v -> (long) v / (1024 * 1024) + " MiB");
  }

  /**
   * Throttles on JVM heap usage.
   *
   * @param config the watermarks
   * @return the sensor
   */
  public static BackpressureSensor heap(HeapBackpressureConfig config) {
    return new HeapSensor(config);
  }
}
