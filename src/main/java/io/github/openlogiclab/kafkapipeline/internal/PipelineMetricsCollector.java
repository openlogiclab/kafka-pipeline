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

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureController;
import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import io.github.openlogiclab.kafkapipeline.metrics.PartitionMetrics;
import io.github.openlogiclab.kafkapipeline.metrics.PipelineMetrics;
import io.github.openlogiclab.kafkapipeline.offset.OffsetStats;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.common.TopicPartition;

/**
 * Internal metrics collector that aggregates pipeline events using lock-free {@link LongAdder}
 * counters.
 *
 * <p>It implements one narrow interface per recording component ({@link PollMetrics}, {@link
 * CommitMetrics}, {@link RebalanceMetrics}, {@link WorkerMetrics}, {@link RetryMetrics}, {@link
 * HandlerAttemptObserver}), and each component depends only on its own. Write methods are called on
 * the hot path (poll loop, worker threads, committer thread) and consist of a single {@code
 * LongAdder} update — cell-striped, no contention, same design as {@link InFlightCounter}.
 *
 * <p>The read method ({@link #snapshot()}) is called on-demand from user code and assembles an
 * immutable {@link PipelineMetrics} record from all counter/gauge values.
 *
 * <p><strong>This is an internal API — not intended for direct use by library consumers.</strong>
 */
public final class PipelineMetricsCollector
    implements PollMetrics,
        CommitMetrics,
        RebalanceMetrics,
        WorkerMetrics,
        RetryMetrics,
        HandlerAttemptObserver {

  private final String name;
  private final InFlightCounter inFlightCounter;
  private final BackpressureController backpressureController;
  private final OffsetStats offsetTracker;

  // ── Throughput counters ────────────────────────────────────────
  private final LongAdder recordsProcessed = new LongAdder();
  private final LongAdder recordsFailed = new LongAdder();
  private final LongAdder recordsSkipped = new LongAdder();
  private final LongAdder pollCount = new LongAdder();
  private final LongAdder emptyPollCount = new LongAdder();
  private final LongAdder handlerAttempts = new LongAdder();
  private final LongAdder handlerNanos = new LongAdder();

  // ── Pressure counters ─────────────────────────────────────────
  private final LongAdder throttleCount = new LongAdder();

  // ── Error counters ────────────────────────────────────────────
  private final LongAdder retryAttempts = new LongAdder();
  private final LongAdder dlqSuccesses = new LongAdder();
  private final LongAdder dlqFailures = new LongAdder();
  private final LongAdder finalFailures = new LongAdder();
  private final LongAdder commitSuccesses = new LongAdder();
  private final LongAdder commitFailures = new LongAdder();

  // ── Rebalance counters ────────────────────────────────────────
  private final LongAdder rebalanceCount = new LongAdder();
  private final LongAdder drainTimeouts = new LongAdder();
  private final LongAdder recordsAbandoned = new LongAdder();
  private final LongAdder processingTimeouts = new LongAdder();

  // ── Partition tracking ────────────────────────────────────────
  private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

  /**
   * Creates a metrics collector.
   *
   * @param name the pipeline name, reported as {@link PipelineMetrics#name()}
   * @param inFlightCounter the in-flight counter
   * @param backpressureController the backpressure controller
   * @param offsetTracker the offset tracker
   */
  public PipelineMetricsCollector(
      String name,
      InFlightCounter inFlightCounter,
      BackpressureController backpressureController,
      OffsetStats offsetTracker) {
    this.name = name;
    this.inFlightCounter = inFlightCounter;
    this.backpressureController = backpressureController;
    this.offsetTracker = offsetTracker;
  }

  // ── Hot-path write methods ────────────────────────────────────

  @Override
  public void recordProcessed(long count) {
    recordsProcessed.add(count);
  }

  /** Adds the attempt to {@link PipelineMetrics#handlerDuration()}. */
  @Override
  public void onAttempt(TopicPartition tp, long nanos, AttemptOutcome outcome) {
    handlerAttempts.increment();
    handlerNanos.add(nanos);
  }

  @Override
  public void recordProcessingTimeout() {
    processingTimeouts.increment();
  }

  @Override
  public void recordFailed(int count) {
    recordsFailed.add(count);
  }

  @Override
  public void recordSkipped(int count) {
    recordsSkipped.add(count);
  }

  @Override
  public void recordPoll() {
    pollCount.increment();
  }

  @Override
  public void recordEmptyPoll() {
    emptyPollCount.increment();
  }

  @Override
  public void recordThrottle() {
    throttleCount.increment();
  }

  @Override
  public void recordRetry() {
    retryAttempts.increment();
  }

  @Override
  public void recordDlqSuccess(int count) {
    dlqSuccesses.add(count);
  }

  @Override
  public void recordDlqFailure(int count) {
    dlqFailures.add(count);
  }

  @Override
  public void recordFinalFailure() {
    finalFailures.increment();
  }

  @Override
  public void recordCommitSuccess() {
    commitSuccesses.increment();
  }

  @Override
  public void recordCommitFailure() {
    commitFailures.increment();
  }

  @Override
  public void recordRebalance() {
    rebalanceCount.increment();
  }

  @Override
  public void recordDrainTimeout() {
    drainTimeouts.increment();
  }

  @Override
  public void recordAbandoned(long count) {
    recordsAbandoned.add(count);
  }

  @Override
  public void partitionAssigned(TopicPartition tp) {
    assignedPartitions.add(tp);
  }

  @Override
  public void partitionRevoked(TopicPartition tp) {
    assignedPartitions.remove(tp);
  }

  // ── Snapshot (cold path) ──────────────────────────────────────

  /**
   * Creates an immutable snapshot of current metrics.
   *
   * @return current metrics snapshot
   */
  public PipelineMetrics snapshot() {
    Map<TopicPartition, PartitionMetrics> partitions = new HashMap<>();
    int windowCapacity = offsetTracker.maxWindowSize();
    for (TopicPartition tp : Set.copyOf(assignedPartitions)) {
      partitions.put(
          tp,
          new PartitionMetricsSnapshot(
              offsetTracker.lag(tp),
              offsetTracker.failureCount(tp),
              offsetTracker.commitStallMillis(tp),
              offsetTracker.windowSize(tp),
              windowCapacity));
    }

    return new PipelineMetricsSnapshot(
        name,
        recordsProcessed.sum(),
        recordsFailed.sum(),
        recordsSkipped.sum(),
        new DurationStatsSnapshot(handlerAttempts.sum(), handlerNanos.sum()),
        pollCount.sum(),
        emptyPollCount.sum(),
        inFlightCounter.records(),
        inFlightCounter.bytes(),
        backpressureController.lastStatus(),
        throttleCount.sum(),
        retryAttempts.sum(),
        dlqSuccesses.sum(),
        dlqFailures.sum(),
        finalFailures.sum(),
        commitSuccesses.sum(),
        commitFailures.sum(),
        processingTimeouts.sum(),
        rebalanceCount.sum(),
        drainTimeouts.sum(),
        recordsAbandoned.sum(),
        partitions);
  }
}
