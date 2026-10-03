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
  private final LongAdder finalFailures = new LongAdder();
  private final LongAdder commitSuccesses = new LongAdder();
  private final LongAdder commitFailures = new LongAdder();

  // ── Rebalance counters ────────────────────────────────────────
  private final LongAdder rebalanceCount = new LongAdder();
  private final LongAdder drainTimeouts = new LongAdder();
  private final LongAdder recordsAbandoned = new LongAdder();

  // ── Partition tracking ────────────────────────────────────────
  private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();

  /**
   * Creates a metrics collector.
   *
   * @param inFlightCounter the in-flight counter
   * @param backpressureController the backpressure controller
   * @param offsetTracker the offset tracker
   */
  public PipelineMetricsCollector(
      InFlightCounter inFlightCounter,
      BackpressureController backpressureController,
      OffsetTracker offsetTracker) {
    this.inFlightCounter = inFlightCounter;
    this.backpressureController = backpressureController;
    this.offsetTracker = offsetTracker;
  }

  // ── Hot-path write methods ────────────────────────────────────

  /**
   * Records successfully processed records.
   *
   * @param count number of records processed
   */
  public void recordProcessed(long count) {
    recordsProcessed.add(count);
  }

  /**
   * Records failed records (after all retries and DLQ attempts).
   *
   * @param count number of records failed
   */
  public void recordFailed(int count) {
    recordsFailed.add(count);
  }

  /**
   * Records skipped records.
   *
   * @param count number of records skipped
   */
  public void recordSkipped(int count) {
    recordsSkipped.add(count);
  }

  /** Records a poll operation. */
  public void recordPoll() {
    pollCount.increment();
  }

  /** Records an empty poll (no records returned). */
  public void recordEmptyPoll() {
    emptyPollCount.increment();
  }

  /** Records a throttle event (backpressure triggered). */
  public void recordThrottle() {
    throttleCount.increment();
  }

  /** Records a retry attempt. */
  public void recordRetry() {
    retryAttempts.increment();
  }

  /**
   * Records successful DLQ sends.
   *
   * @param count number of records sent to DLQ
   */
  public void recordDlqSuccess(int count) {
    dlqSuccesses.add(count);
  }

  /**
   * Records failed DLQ sends.
   *
   * @param count number of records that failed to send to DLQ
   */
  public void recordDlqFailure(int count) {
    dlqFailures.add(count);
  }

  /** Records a final failure (record dropped after all recovery attempts). */
  public void recordFinalFailure() {
    finalFailures.increment();
  }

  /** Records a successful offset commit. */
  public void recordCommitSuccess() {
    commitSuccesses.increment();
  }

  /** Records a failed offset commit. */
  public void recordCommitFailure() {
    commitFailures.increment();
  }

  /** Records a rebalance event. */
  public void recordRebalance() {
    rebalanceCount.increment();
  }

  /** Records a drain timeout event. */
  public void recordDrainTimeout() {
    drainTimeouts.increment();
  }

  /**
   * Records abandoned records during drain.
   *
   * @param count number of records abandoned
   */
  public void recordAbandoned(long count) {
    recordsAbandoned.add(count);
  }

  /**
   * Records a partition assignment.
   *
   * @param tp the assigned partition
   */
  public void partitionAssigned(TopicPartition tp) {
    assignedPartitions.add(tp);
  }

  /**
   * Records a partition revocation.
   *
   * @param tp the revoked partition
   */
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
    Map<TopicPartition, Long> lags = new HashMap<>();
    Map<TopicPartition, Integer> failures = new HashMap<>();
    for (TopicPartition tp : Set.copyOf(assignedPartitions)) {
      lags.put(tp, offsetTracker.lag(tp));
      int failureCount = offsetTracker.failureCount(tp);
      if (failureCount > 0) {
        failures.put(tp, failureCount);
      }
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
        Map.copyOf(failures),
        retryAttempts.sum(),
        dlqSuccesses.sum(),
        dlqFailures.sum(),
        finalFailures.sum(),
        commitSuccesses.sum(),
        commitFailures.sum(),
        rebalanceCount.sum(),
        drainTimeouts.sum(),
        recordsAbandoned.sum(),
        Set.copyOf(assignedPartitions));
  }
}
