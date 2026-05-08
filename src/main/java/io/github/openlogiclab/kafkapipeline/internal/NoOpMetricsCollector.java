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

import io.github.openlogiclab.kafkapipeline.PipelineMetrics;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Null-object implementation of {@link PipelineMetricsCollector} that silently discards all events.
 * Used by backward-compatible constructors that don't receive a real collector.
 */
public final class NoOpMetricsCollector extends PipelineMetricsCollector {

  public static final NoOpMetricsCollector INSTANCE = new NoOpMetricsCollector();

  private static final PipelineMetrics EMPTY =
      new PipelineMetrics(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          BackpressureStatus.OK,
          0,
          Map.of(),
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          Set.of());

  private NoOpMetricsCollector() {
    super(null, null, null);
  }

  @Override
  public void recordProcessed(long count) {}

  @Override
  public void recordFailed() {}

  @Override
  public void recordSkipped() {}

  @Override
  public void recordPoll() {}

  @Override
  public void recordEmptyPoll() {}

  @Override
  public void recordThrottle() {}

  @Override
  public void recordRetry() {}

  @Override
  public void recordDlqSuccess() {}

  @Override
  public void recordDlqFailure() {}

  @Override
  public void recordPartitionFailure() {}

  @Override
  public void recordCommitSuccess() {}

  @Override
  public void recordCommitFailure() {}

  @Override
  public void recordRebalance() {}

  @Override
  public void recordDrainTimeout() {}

  @Override
  public void recordAbandoned(long count) {}

  @Override
  public void partitionAssigned(TopicPartition tp) {}

  @Override
  public void partitionRevoked(TopicPartition tp) {}

  @Override
  public PipelineMetrics snapshot() {
    return EMPTY;
  }
}
