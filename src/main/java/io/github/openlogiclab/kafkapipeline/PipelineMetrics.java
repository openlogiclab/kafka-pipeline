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
package io.github.openlogiclab.kafkapipeline;

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureStatus;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Point-in-time snapshot of pipeline operational metrics.
 *
 * <p>Obtained on-demand via {@link KafkaPipeline#metrics()}. Each call captures the current values
 * of all internal counters — no background threads or timers involved. The snapshot is immutable
 * and safe to pass across threads, serialize, or log.
 *
 * <h2>Counter vs. Gauge semantics</h2>
 *
 * <ul>
 *   <li><b>Counters</b> (e.g. {@code recordsProcessed}) are monotonically increasing cumulative
 *       totals since pipeline start. Compute rates by diffing two snapshots: {@code
 *       (current.recordsProcessed() - previous.recordsProcessed()) / elapsedSeconds}.
 *   <li><b>Gauges</b> (e.g. {@code inFlightRecords}) are instantaneous values that go up and down.
 *       Read them directly.
 * </ul>
 *
 * <h2>Consistency</h2>
 *
 * <p>Individual fields are accurate, but the snapshot is <em>not</em> atomic across all fields —
 * two counters may reflect slightly different points in time. This is standard for lock-free
 * metrics and sufficient for monitoring and alerting.
 *
 * <h2>Diagnostic guide</h2>
 *
 * <table>
 *   <caption>Common production scenarios and which metrics to check</caption>
 *   <tr><th>Symptom</th><th>Check</th><th>Likely cause</th></tr>
 *   <tr>
 *     <td>Throughput drops to zero</td>
 *     <td>{@code backpressureStatus}, {@code inFlightRecords}</td>
 *     <td>Backpressure triggered — workers are slower than poll rate</td>
 *   </tr>
 *   <tr>
 *     <td>Consumer lag growing</td>
 *     <td>{@code partitionLags}, {@code retryAttempts}</td>
 *     <td>Retry storms or slow handler blocking workers</td>
 *   </tr>
 *   <tr>
 *     <td>Frequent rebalances</td>
 *     <td>{@code rebalanceCount}, {@code drainTimeouts}</td>
 *     <td>{@code max.poll.interval.ms} too low, or handler exceeds session timeout</td>
 *   </tr>
 *   <tr>
 *     <td>Data loss suspected</td>
 *     <td>{@code recordsSkipped}, {@code dlqSuccesses}</td>
 *     <td>Fallback=SKIP is dropping records; check DLQ topic</td>
 *   </tr>
 *   <tr>
 *     <td>Memory pressure / OOM</td>
 *     <td>{@code inFlightRecords}, {@code inFlightBytes}</td>
 *     <td>Backpressure thresholds too high for available heap</td>
 *   </tr>
 * </table>
 *
 * <h2>Export examples</h2>
 *
 * <p>The library collects metrics; you decide how to export them:
 *
 * <pre>{@code
 * // Periodic logging
 * scheduler.scheduleAtFixedRate(() -> {
 *     logger.info("pipeline: {}", pipeline.metrics());
 * }, 0, 30, TimeUnit.SECONDS);
 *
 * // Prometheus gauge
 * Gauge inFlight = Gauge.build("kafka_pipeline_in_flight_records", "...").register();
 * scheduler.scheduleAtFixedRate(() -> {
 *     inFlight.set(pipeline.metrics().inFlightRecords());
 * }, 0, 15, TimeUnit.SECONDS);
 *
 * // Health-check endpoint
 * app.get("/health", ctx -> {
 *     PipelineMetrics m = pipeline.metrics();
 *     ctx.status(m.backpressureStatus() == BackpressureStatus.CRITICAL ? 503 : 200);
 *     ctx.json(m);
 * });
 * }</pre>
 *
 * @see KafkaPipeline#metrics()
 */
public record PipelineMetrics(

    // ── Throughput ──────────────────────────────────────────────

    /**
     * Total records successfully processed since pipeline start. (Counter)
     *
     * <p>Includes records acked after handler success and records resolved via DLQ or skip. Does
     * not include records skipped by {@link
     * io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook#beforeProcess} — those
     * are counted separately in {@link #recordsSkipped}.
     */
    long recordsProcessed,

    /** Total records that triggered a partition failure since pipeline start. (Counter) */
    long recordsFailed,

    /**
     * Total records skipped since pipeline start. (Counter)
     *
     * <p>Includes records skipped by the lifecycle hook ({@code beforeProcess} returning {@code
     * false}) and records skipped by the {@link io.github.openlogiclab.kafkapipeline.error.Fallback
     * SKIP} fallback.
     */
    long recordsSkipped,

    /**
     * Total {@code consumer.poll()} calls since pipeline start. (Counter)
     *
     * <p>Useful for computing average batch size: {@code recordsProcessed / (pollCount -
     * emptyPollCount)}.
     */
    long pollCount,

    /**
     * Total poll cycles that returned zero records since pipeline start. (Counter)
     *
     * <p>A high ratio of {@code emptyPollCount / pollCount} indicates the consumer is polling
     * faster than records arrive.
     */
    long emptyPollCount,

    // ── Pressure ───────────────────────────────────────────────

    /**
     * Current number of records between poll and ack/fail. (Gauge)
     *
     * <p>High values indicate workers are falling behind. Compare against backpressure thresholds
     * configured in {@link io.github.openlogiclab.kafkapipeline.backpressure.BackpressureConfig}.
     */
    int inFlightRecords,

    /**
     * Current total estimated bytes of in-flight records. (Gauge)
     *
     * <p>Based on {@code ConsumerRecord.serializedKeySize() + serializedValueSize()}.
     */
    long inFlightBytes,

    /** Current backpressure level as evaluated by all configured sensors. (Gauge) */
    BackpressureStatus backpressureStatus,

    /**
     * Total number of times backpressure transitioned from OK to THROTTLE or CRITICAL since
     * pipeline start. (Counter)
     *
     * <p>Frequent throttle events indicate the pipeline is operating near capacity.
     */
    long throttleCount,

    /**
     * Per-partition processing lag — distance between the highest registered offset and the
     * committable offset. (Gauge)
     *
     * <p>The returned map is an unmodifiable snapshot.
     */
    Map<TopicPartition, Long> partitionLags,

    // ── Errors ──────────────────────────────────────────────────

    /**
     * Total retry attempts across all records since pipeline start. (Counter)
     *
     * <p>A single record retried 3 times contributes 3 to this counter. A sustained high rate
     * relative to {@code recordsProcessed} indicates a systemic downstream issue.
     */
    long retryAttempts,

    /** Total records successfully sent to DLQ since pipeline start. (Counter) */
    long dlqSuccesses,

    /** Total DLQ send failures since pipeline start. (Counter) */
    long dlqFailures,

    /**
     * Total times a partition entered failed state due to {@link
     * io.github.openlogiclab.kafkapipeline.error.Fallback#FAIL_PARTITION FAIL_PARTITION} since
     * pipeline start. (Counter)
     */
    long partitionFailures,

    /** Total successful offset commits (async + sync) since pipeline start. (Counter) */
    long commitSuccesses,

    /** Total failed offset commits (async + sync) since pipeline start. (Counter) */
    long commitFailures,

    // ── Rebalance ──────────────────────────────────────────────

    /**
     * Number of consumer group rebalances since pipeline start. (Counter)
     *
     * <p>Frequent rebalances (more than a few per hour) usually indicate {@code
     * max.poll.interval.ms} is too short for the handler's processing time, or unstable network
     * causing session timeouts.
     */
    long rebalanceCount,

    /**
     * Total partition drains that did not complete within the configured drain timeout since
     * pipeline start. (Counter)
     *
     * <p>Indicates in-flight records were abandoned during rebalance and will be re-consumed by the
     * new owner.
     */
    long drainTimeouts,

    /**
     * Total records abandoned (not yet processed) during rebalance drain since pipeline start.
     * (Counter)
     */
    long recordsAbandoned,

    /**
     * Set of partitions currently assigned to this consumer. (Gauge)
     *
     * <p>The returned set is an unmodifiable snapshot.
     */
    Set<TopicPartition> assignedPartitions) {}
