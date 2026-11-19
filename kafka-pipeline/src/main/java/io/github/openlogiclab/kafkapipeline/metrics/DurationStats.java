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
package io.github.openlogiclab.kafkapipeline.metrics;

import io.github.openlogiclab.kafkapipeline.internal.DurationStatsSnapshot;
import java.time.Duration;

/**
 * Cumulative count and total time of a timed operation since pipeline start. (Counters)
 *
 * <p>Compute the mean over an interval by diffing two snapshots: {@code (total₂ - total₁) / (count₂
 * - count₁)}. For percentiles, use a metrics exporter, which records each duration into a native
 * histogram.
 *
 * <p>This interface is sealed: only the pipeline creates instances.
 */
public sealed interface DurationStats permits DurationStatsSnapshot {

  /**
   * Number of timed operations.
   *
   * @return the count
   */
  long count();

  /**
   * Total time of all timed operations, in nanoseconds.
   *
   * @return the total in nanoseconds
   */
  long totalNanos();

  /**
   * Total time of all timed operations.
   *
   * @return the total
   */
  default Duration total() {
    return Duration.ofNanos(totalNanos());
  }

  /**
   * Mean time since pipeline start, or zero if nothing was timed.
   *
   * @return the mean
   */
  default Duration mean() {
    return count() > 0 ? Duration.ofNanos(totalNanos() / count()) : Duration.ZERO;
  }
}
