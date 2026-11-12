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

import io.github.openlogiclab.kafkapipeline.PipelineConfig;
import io.github.openlogiclab.kafkapipeline.internal.PartitionMetricsSnapshot;

/**
 * Point-in-time metrics for one assigned partition, from {@link PipelineMetrics#partitions()}.
 *
 * <p>Values describe the partition since it was last assigned to this consumer; they start over
 * after a rebalance moves the partition away and back.
 *
 * <p>This interface is sealed: only the pipeline creates instances, so new metrics can be added in
 * later releases without breaking code that reads them.
 */
public sealed interface PartitionMetrics permits PartitionMetricsSnapshot {

  /**
   * Distance between the highest polled offset and the committable offset. (Gauge)
   *
   * @return records polled but not yet committable
   */
  long lag();

  /**
   * Unrecoverable failures on this partition since it was assigned. (Counter)
   *
   * @return failure count
   */
  int failures();

  /**
   * Time, in milliseconds, since the committable offset last moved while records are pending or in
   * progress; 0 when nothing is waiting. (Gauge)
   *
   * <p>A value that keeps growing means a record at the head of the partition is stuck: commits are
   * held back, and the partition pauses once its window fills. Alert on it, and consider {@code
   * processingTimeout}.
   *
   * @return commit stall in milliseconds
   */
  long commitStallMillis();

  /**
   * Entries held in the partition's offset window, including finished offsets waiting behind an
   * unfinished one. (Gauge)
   *
   * @return window size
   */
  int windowSize();

  /**
   * Maximum entries the window can hold before the partition is paused, from {@link
   * PipelineConfig#maxWindowSize()}.
   *
   * @return window capacity
   */
  int windowCapacity();

  /**
   * Fraction of the window in use, from 0.0 to 1.0. (Gauge)
   *
   * <p>Approaching 1.0 means the partition is about to pause. This is an earlier warning than lag.
   *
   * @return window fill ratio
   */
  default double windowFill() {
    return windowCapacity() > 0 ? (double) windowSize() / windowCapacity() : 0.0;
  }
}
