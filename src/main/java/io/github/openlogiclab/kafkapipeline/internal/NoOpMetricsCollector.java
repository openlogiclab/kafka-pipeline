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

import org.apache.kafka.common.TopicPartition;

/**
 * Discards every metric. For components built without a pipeline, mainly in tests.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class NoOpMetricsCollector
    implements PollMetrics, CommitMetrics, RebalanceMetrics, WorkerMetrics, RetryMetrics {

  /** Singleton instance. */
  public static final NoOpMetricsCollector INSTANCE = new NoOpMetricsCollector();

  private NoOpMetricsCollector() {}

  @Override
  public void recordPoll() {}

  @Override
  public void recordEmptyPoll() {}

  @Override
  public void recordThrottle() {}

  @Override
  public void recordCommitSuccess() {}

  @Override
  public void recordCommitFailure() {}

  @Override
  public void partitionAssigned(TopicPartition tp) {}

  @Override
  public void partitionRevoked(TopicPartition tp) {}

  @Override
  public void recordRebalance() {}

  @Override
  public void recordDrainTimeout() {}

  @Override
  public void recordAbandoned(long count) {}

  @Override
  public void recordProcessed(long count) {}

  @Override
  public void recordSkipped(int count) {}

  @Override
  public void recordRetry() {}

  @Override
  public void recordProcessingTimeout() {}

  @Override
  public void recordDlqSuccess(int count) {}

  @Override
  public void recordDlqFailure(int count) {}

  @Override
  public void recordFailed(int count) {}

  @Override
  public void recordFinalFailure() {}
}
