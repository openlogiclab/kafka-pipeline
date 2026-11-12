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
package io.github.openlogiclab.kafkapipeline.internal;

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.metrics.DurationStats;
import io.github.openlogiclab.kafkapipeline.metrics.PartitionMetrics;
import io.github.openlogiclab.kafkapipeline.metrics.PipelineMetrics;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

/**
 * The only {@link PipelineMetrics} implementation. Each component is documented on the matching
 * {@link PipelineMetrics} method.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 * Its constructor changes whenever a metric is added.
 *
 * @param name see {@link PipelineMetrics#name()}
 * @param recordsProcessed see {@link PipelineMetrics#recordsProcessed()}
 * @param recordsFailed see {@link PipelineMetrics#recordsFailed()}
 * @param recordsSkipped see {@link PipelineMetrics#recordsSkipped()}
 * @param handlerDuration see {@link PipelineMetrics#handlerDuration()}
 * @param pollCount see {@link PipelineMetrics#pollCount()}
 * @param emptyPollCount see {@link PipelineMetrics#emptyPollCount()}
 * @param inFlightRecords see {@link PipelineMetrics#inFlightRecords()}
 * @param inFlightBytes see {@link PipelineMetrics#inFlightBytes()}
 * @param backpressureStatus see {@link PipelineMetrics#backpressureStatus()}
 * @param throttleCount see {@link PipelineMetrics#throttleCount()}
 * @param retryAttempts see {@link PipelineMetrics#retryAttempts()}
 * @param dlqSuccesses see {@link PipelineMetrics#dlqSuccesses()}
 * @param dlqFailures see {@link PipelineMetrics#dlqFailures()}
 * @param finalFailures see {@link PipelineMetrics#finalFailures()}
 * @param commitSuccesses see {@link PipelineMetrics#commitSuccesses()}
 * @param commitFailures see {@link PipelineMetrics#commitFailures()}
 * @param processingTimeouts see {@link PipelineMetrics#processingTimeouts()}
 * @param rebalanceCount see {@link PipelineMetrics#rebalanceCount()}
 * @param drainTimeouts see {@link PipelineMetrics#drainTimeouts()}
 * @param recordsAbandoned see {@link PipelineMetrics#recordsAbandoned()}
 * @param partitions see {@link PipelineMetrics#partitions()}
 */
public record PipelineMetricsSnapshot(
    String name,
    long recordsProcessed,
    long recordsFailed,
    long recordsSkipped,
    DurationStats handlerDuration,
    long pollCount,
    long emptyPollCount,
    int inFlightRecords,
    long inFlightBytes,
    BackpressureStatus backpressureStatus,
    long throttleCount,
    long retryAttempts,
    long dlqSuccesses,
    long dlqFailures,
    long finalFailures,
    long commitSuccesses,
    long commitFailures,
    long processingTimeouts,
    long rebalanceCount,
    long drainTimeouts,
    long recordsAbandoned,
    Map<TopicPartition, PartitionMetrics> partitions)
    implements PipelineMetrics {

  /** Copies {@code partitions} so the snapshot stays immutable. */
  public PipelineMetricsSnapshot {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(handlerDuration, "handlerDuration");
    Objects.requireNonNull(backpressureStatus, "backpressureStatus");
    partitions = Map.copyOf(partitions);
  }
}
