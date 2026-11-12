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

import io.github.openlogiclab.kafkapipeline.KafkaPipeline;
import io.github.openlogiclab.kafkapipeline.PipelineConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsSnapshot;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Point-in-time snapshot of pipeline operational metrics.
 *
 * <p>Obtained on demand via {@link KafkaPipeline#metrics()}. Each call captures the current values
 * of all internal counters; no background threads or timers are involved. The snapshot is immutable
 * and safe to pass across threads or log.
 *
 * <p>This interface is sealed: only the pipeline creates snapshots, so new metrics can be added in
 * later releases without breaking code that reads them.
 *
 * <h2>Counter vs. gauge semantics</h2>
 *
 * <ul>
 *   <li><b>Counters</b> (e.g. {@code recordsProcessed}) are monotonically increasing cumulative
 *       totals since pipeline start. Compute rates by diffing two snapshots: {@code
 *       (current.recordsProcessed() - previous.recordsProcessed()) / elapsedSeconds}.
 *   <li><b>Gauges</b> (e.g. {@code inFlightRecords}) are instantaneous values that go up and down.
 *       Read them directly.
 * </ul>
 *
 * <h2>Record outcomes</h2>
 *
 * <p>Every record the pipeline finishes is counted in exactly one of {@link #recordsProcessed},
 * {@link #recordsSkipped}, {@link #dlqSuccesses} or {@link #recordsFailed}. Records left unfinished
 * by shutdown or rebalance are not counted in any of them; they are redelivered (see {@link
 * #recordsAbandoned}).
 *
 * <h2>Consistency</h2>
 *
 * <p>Individual values are accurate, but the snapshot is <em>not</em> atomic across all values: two
 * counters may reflect slightly different points in time. This is standard for lock-free metrics
 * and sufficient for monitoring and alerting.
 *
 * <h2>Diagnostic guide</h2>
 *
 * <table>
 *   <caption>Common production scenarios and which metrics to check</caption>
 *   <tr><th>Symptom</th><th>Check</th><th>Likely cause</th></tr>
 *   <tr>
 *     <td>Throughput drops to zero</td>
 *     <td>{@code backpressureStatus}, {@code inFlightRecords}</td>
 *     <td>Backpressure triggered: workers are slower than the poll rate</td>
 *   </tr>
 *   <tr>
 *     <td>Consumer lag growing</td>
 *     <td>{@code partitions().get(tp).lag()}, {@code retryAttempts}</td>
 *     <td>Retry storms or a slow handler blocking workers</td>
 *   </tr>
 *   <tr>
 *     <td>One partition stops committing</td>
 *     <td>{@code partitions().get(tp).commitStallMillis()}, {@code windowFill()}</td>
 *     <td>A record at the head of the partition is stuck; consider {@code processingTimeout}</td>
 *   </tr>
 *   <tr>
 *     <td>Frequent rebalances</td>
 *     <td>{@code rebalanceCount}, {@code drainTimeouts}</td>
 *     <td>{@code max.poll.interval.ms} too low, or handler exceeds session timeout</td>
 *   </tr>
 *   <tr>
 *     <td>Data loss suspected</td>
 *     <td>{@code recordsFailed}, {@code dlqFailures}</td>
 *     <td>Records dropped after retries; check the DLQ and the final failure logs</td>
 *   </tr>
 *   <tr>
 *     <td>Memory pressure / OOM</td>
 *     <td>{@code inFlightRecords}, {@code inFlightBytes}</td>
 *     <td>Backpressure thresholds too high for available heap</td>
 *   </tr>
 * </table>
 *
 * @see KafkaPipeline#metrics()
 * @see PartitionMetrics
 */
public sealed interface PipelineMetrics permits PipelineMetricsSnapshot {

  /**
   * Name of the pipeline these metrics belong to, from {@link PipelineConfig#name()}. Use it to
   * tell pipelines apart when several run in one JVM.
   *
   * @return the pipeline name
   */
  String name();

  // ── Throughput ──────────────────────────────────────────────

  /**
   * Total records whose handler succeeded since pipeline start. (Counter)
   *
   * @return records processed
   */
  long recordsProcessed();

  /**
   * Total records dropped since pipeline start: all retries failed and the record could not be sent
   * to the DLQ, either because none is configured or because the send failed. (Counter)
   *
   * <p>This is data loss, so it is the counter to alert on. The {@link
   * io.github.openlogiclab.kafkapipeline.error.FinalFailureHandler} was invoked for each of these
   * records.
   *
   * @return records dropped
   */
  long recordsFailed();

  /**
   * Total records skipped by the lifecycle hook ({@code beforeProcess} returning {@code false})
   * since pipeline start. (Counter)
   *
   * <p>This is intentional filtering, not a failure. Records dropped after a failure are counted in
   * {@link #recordsFailed} instead.
   *
   * @return records skipped by the hook
   */
  long recordsSkipped();

  /**
   * Time spent in the handler, one entry per handler attempt. A batch handler call counts once;
   * retries count separately; failed and timed-out attempts are included.
   *
   * <p>Compare the mean with {@code processingTimeout}: a mean close to it means healthy records
   * are being interrupted.
   *
   * @return handler attempt count and total time
   */
  DurationStats handlerDuration();

  /**
   * Total {@code consumer.poll()} calls since pipeline start. (Counter)
   *
   * @return poll calls
   */
  long pollCount();

  /**
   * Total polls that returned zero records since pipeline start. (Counter)
   *
   * <p>A high ratio of {@code emptyPollCount / pollCount} means the consumer polls faster than
   * records arrive.
   *
   * @return empty polls
   */
  long emptyPollCount();

  // ── Pressure ───────────────────────────────────────────────

  /**
   * Current number of records between poll and ack or failure. (Gauge)
   *
   * @return in-flight records
   */
  int inFlightRecords();

  /**
   * Current estimated bytes of in-flight records, from {@code serializedKeySize() +
   * serializedValueSize()}. (Gauge)
   *
   * @return in-flight bytes
   */
  long inFlightBytes();

  /**
   * Current backpressure level as evaluated by all configured sensors. (Gauge)
   *
   * @return the backpressure status
   */
  BackpressureStatus backpressureStatus();

  /**
   * Total times backpressure went from OK to THROTTLE or CRITICAL since pipeline start. (Counter)
   *
   * @return throttle events
   */
  long throttleCount();

  // ── Errors ──────────────────────────────────────────────────

  /**
   * Total retry attempts across all records since pipeline start. A record retried 3 times adds 3.
   * (Counter)
   *
   * @return retry attempts
   */
  long retryAttempts();

  /**
   * Total records sent to the DLQ since pipeline start. (Counter)
   *
   * <p>When a batch's DLQ send fails partway, the records sent before the failure are counted here
   * and the rest in {@link #recordsFailed}.
   *
   * @return records sent to the DLQ
   */
  long dlqSuccesses();

  /**
   * Total records whose DLQ send failed since pipeline start. (Counter)
   *
   * @return failed DLQ sends
   */
  long dlqFailures();

  /**
   * Total final failure handler invocations since pipeline start; one per record in {@link
   * #recordsFailed}. (Counter)
   *
   * @return final failure handler invocations
   */
  long finalFailures();

  /**
   * Total successful offset commits (async and sync) since pipeline start. (Counter)
   *
   * @return successful commits
   */
  long commitSuccesses();

  /**
   * Total failed offset commits (async and sync) since pipeline start. (Counter)
   *
   * @return failed commits
   */
  long commitFailures();

  /**
   * Total handler attempts interrupted by {@code processingTimeout} since pipeline start. (Counter)
   *
   * @return processing timeouts
   */
  long processingTimeouts();

  // ── Rebalance ──────────────────────────────────────────────

  /**
   * Consumer group rebalances since pipeline start. (Counter)
   *
   * <p>More than a few per hour usually means {@code max.poll.interval.ms} is too short for the
   * handler, or an unstable network is causing session timeouts.
   *
   * @return rebalances
   */
  long rebalanceCount();

  /**
   * Partition drains that did not finish within the drain timeout since pipeline start. Their
   * in-flight records are re-consumed by the new owner. (Counter)
   *
   * @return drain timeouts
   */
  long drainTimeouts();

  /**
   * Records abandoned (not yet processed) during rebalance drain since pipeline start. (Counter)
   *
   * @return abandoned records
   */
  long recordsAbandoned();

  // ── Partitions ─────────────────────────────────────────────

  /**
   * Metrics for every partition currently assigned to this consumer, including idle ones, so each
   * partition always reports a value. Revoked partitions are removed. (Gauges)
   *
   * @return an unmodifiable map from partition to its metrics
   */
  Map<TopicPartition, PartitionMetrics> partitions();

  /**
   * Partitions currently assigned to this consumer: the keys of {@link #partitions()}. (Gauge)
   *
   * @return an unmodifiable set of assigned partitions
   */
  default Set<TopicPartition> assignedPartitions() {
    return partitions().keySet();
  }
}
