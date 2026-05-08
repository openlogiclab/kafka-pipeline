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

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.PipelineMetrics;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureController;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
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
 * <p>Write methods ({@code recordXxx}) are called on the hot path (poll loop, worker threads,
 * committer thread) and consist of a single {@code LongAdder.increment()} — cell-striped, no
 * contention, same design as {@link InFlightCounter}.
 *
 * <p>The read method ({@link #snapshot()}) is called on-demand from user code and assembles an
 * immutable {@link PipelineMetrics} record from all counter/gauge values.
 *
 * <p><strong>This is an internal API — not intended for direct use by library consumers.</strong>
 */
public class PipelineMetricsCollector {

  private final InFlightCounter inFlightCounter;
  private final BackpressureController backpressureController;
  private final OffsetTracker offsetTracker;

  // ── Throughput counters ────────────────────────────────────────
  private final LongAdder recordsProcessed = new LongAdder();
  private final LongAdder recordsFailed = new LongAdder();
  private final LongAdder recordsSkipped = new LongAdder();
  private final LongAdder pollCount = new LongAdder();
  private final LongAdder emptyPollCount = new LongAdder();

  // ── Pressure counters ─────────────────────────────────────────
  private final LongAdder throttleCount = new LongAdder();

  // ── Error counters ────────────────────────────────────────────
  private final LongAdder retryAttempts = new LongAdder();
  private final LongAdder dlqSuccesses = new LongAdder();
  private final LongAdder dlqFailures = new LongAdder();
  private final LongAdder partitionFailures = new LongAdder();
  private final LongAdder commitSuccesses = new LongAdder();
  private final LongAdder commitFailures = new LongAdder();

  // ── Rebalance counters ────────────────────────────────────────
  private final LongAdder rebalanceCount = new LongAdder();
  private final LongAdder drainTimeouts = new LongAdder();
  private final LongAdder recordsAbandoned = new LongAdder();

  // ── Partition tracking ────────────────────────────────────────
  private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

  public PipelineMetricsCollector(
      InFlightCounter inFlightCounter,
      BackpressureController backpressureController,
      OffsetTracker offsetTracker) {
    this.inFlightCounter = inFlightCounter;
    this.backpressureController = backpressureController;
    this.offsetTracker = offsetTracker;
  }

  // ── Hot-path write methods ────────────────────────────────────

  public void recordProcessed(long count) {
    recordsProcessed.add(count);
  }

  public void recordFailed() {
    recordsFailed.increment();
  }

  public void recordSkipped() {
    recordsSkipped.increment();
  }

  public void recordPoll() {
    pollCount.increment();
  }

  public void recordEmptyPoll() {
    emptyPollCount.increment();
  }

  public void recordThrottle() {
    throttleCount.increment();
  }

  public void recordRetry() {
    retryAttempts.increment();
  }

  public void recordDlqSuccess() {
    dlqSuccesses.increment();
  }

  public void recordDlqFailure() {
    dlqFailures.increment();
  }

  public void recordPartitionFailure() {
    partitionFailures.increment();
  }

  public void recordCommitSuccess() {
    commitSuccesses.increment();
  }

  public void recordCommitFailure() {
    commitFailures.increment();
  }

  public void recordRebalance() {
    rebalanceCount.increment();
  }

  public void recordDrainTimeout() {
    drainTimeouts.increment();
  }

  public void recordAbandoned(long count) {
    recordsAbandoned.add(count);
  }

  public void partitionAssigned(TopicPartition tp) {
    assignedPartitions.add(tp);
  }

  public void partitionRevoked(TopicPartition tp) {
    assignedPartitions.remove(tp);
  }

  // ── Snapshot (cold path) ──────────────────────────────────────

  public PipelineMetrics snapshot() {
    Map<TopicPartition, Long> lags = new HashMap<>();
    for (TopicPartition tp : Set.copyOf(assignedPartitions)) {
      lags.put(tp, offsetTracker.lag(tp));
    }

    return new PipelineMetrics(
        recordsProcessed.sum(),
        recordsFailed.sum(),
        recordsSkipped.sum(),
        pollCount.sum(),
        emptyPollCount.sum(),
        inFlightCounter.records(),
        inFlightCounter.bytes(),
        backpressureController.evaluate(),
        throttleCount.sum(),
        Map.copyOf(lags),
        retryAttempts.sum(),
        dlqSuccesses.sum(),
        dlqFailures.sum(),
        partitionFailures.sum(),
        commitSuccesses.sum(),
        commitFailures.sum(),
        rebalanceCount.sum(),
        drainTimeouts.sum(),
        recordsAbandoned.sum(),
        Set.copyOf(assignedPartitions));
  }
}
