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

import io.github.openlogiclab.kafkapipeline.internal.backpressure.WatermarkSensor;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * One source of backpressure, such as the number of records in flight, unsent producer records, or
 * a downstream connection pool. Add your own with {@code addBackpressureSensor(...)} on the
 * pipeline builder, or return one from a sink's {@code sensor()}.
 *
 * <p>Backpressure is global: if any sensor says {@link BackpressureStatus#THROTTLE}, the pipeline
 * pauses fetching on every partition, because pausing one partition does not relieve pressure on
 * shared workers or memory.
 *
 * <p>{@link #evaluate()} is called by the poll thread only, once per poll-loop iteration, so it may
 * keep state (for example hysteresis) without synchronization. It must be cheap: it runs before
 * every poll. {@link #statusDetail()} may be called from any thread, for logs.
 *
 * @see #watermark(String, LongSupplier, long, long)
 */
public interface BackpressureSensor {

  /**
   * Evaluates the current pressure. Called by the poll thread only; may update internal state.
   *
   * @return the current status
   */
  BackpressureStatus evaluate();

  /**
   * Returns a short identifier for logs, e.g. {@code "record-count"}.
   *
   * @return the sensor name
   */
  String name();

  /**
   * Returns a human-readable description of the current reading, e.g. {@code "record-count: 12000
   * records (high=10000 records, low=6000 records)"}. Must not change state.
   *
   * @return status detail
   */
  String statusDetail();

  /**
   * Returns a sensor over any count of pending work, such as unsent producer records or requests in
   * flight to a service. It throttles when the count reaches {@code high} and resumes once it has
   * dropped to {@code low}, the same hysteresis as the built-in sensors.
   *
   * <pre>{@code
   * BackpressureSensor.watermark("search-pending", client::pendingRequests, 2_000, 1_000)
   * }</pre>
   *
   * @param name short name for logs, e.g. {@code "kafka-sink-pending"}
   * @param count supplies the current count; must be cheap and thread-safe
   * @param high count at which to throttle; positive
   * @param low count at which to resume; at least 0 and less than {@code high}
   * @return the sensor
   */
  static BackpressureSensor watermark(String name, LongSupplier count, long high, long low) {
    Objects.requireNonNull(count, "count");
    if (high <= 0 || low < 0 || low >= high) {
      throw new IllegalArgumentException(
          "need 0 <= low < high and high > 0, got low=" + low + ", high=" + high);
    }
    return new WatermarkSensor(name, count::getAsLong, high, low, v -> (long) v + " pending");
  }
}
