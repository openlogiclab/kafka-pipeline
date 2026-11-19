# kafka-pipeline 0.3.0

Changes since 0.2.0 (#64 and the sink and readability work that follows it).

> **Versioning:** kafka-pipeline is pre-1.0. Following [Semantic Versioning](https://semver.org/#spec-item-4), a 0.x minor release can include breaking changes; they are always listed below. Patch releases (0.3.x) contain no breaking changes. Pin an exact minor version and read these notes before upgrading.

## Breaking changes

### Renamed or removed

| 0.2.0 | 0.3.0 | Why |
|---|---|---|
| `Fallback.SKIP`, `Fallback.DLQ_THEN_SKIP` | `Fallback.DROP`, `Fallback.DLQ_THEN_DROP` | "Skip" also meant the hook skipping a record; a failed record is *dropped* |
| `ErrorStrategy.skipOnError()` | `ErrorStrategy.dropOnError()` | Same |
| `PipelineMetrics.recordsFailed()` | `recordsDropped()` | Same |
| `PipelineMetrics.finalFailures()` | removed; use `recordsDropped()` | It always equalled the number of dropped records |
| `PipelineMetrics.partitionLags()`, `partitionFailures()`, `partitionCommitStallMillis()` | `partitions().get(tp)` → `uncommittedOffsets()`, `dropped()`, `commitStallMillis()` | One `PartitionMetrics` per partition (#64); "lag" read as Kafka consumer lag, which it is not |
| `io.github.openlogiclab.kafkapipeline.PipelineMetrics` | `io.github.openlogiclab.kafkapipeline.metrics.PipelineMetrics` | Metrics types have their own package (#64) |
| Builder `backpressure(BackpressureConfig)` | `recordBackpressure(RecordBackpressureConfig)` | The config only covers the record count |
| `BackpressureStatus.CRITICAL`, every `criticalThreshold*` setting | removed | CRITICAL had exactly the same effect as THROTTLE |
| `HeapBackpressureConfig` `throttleThreshold`, `resumeThreshold` | `highWatermark`, `lowWatermark` | Same names as the other backpressure configs |
| `enabled(boolean)` on the backpressure config builders | removed; use `disabled()` | A builder now always builds an enabled config |
| `BackpressureSensor.currentStatus()` | `evaluate()` | It advances the sensor's state; poll thread only |
| `RetryExecutor.FailureResolution.INTERRUPTED` (internal) | `ABANDONED` | Also covers a revoked partition and a fatal DLQ |
| `InFlightCounter`, `BackpressureController`, `HeapSensor`, `RecordCountSensor`, `ByteSizeSensor` (package `backpressure`) | internal | The public package now holds only the settings and the sensor interface |
| `PipelineConfig` public constructor, record `equals`/`hashCode` | removed; use `PipelineConfig.builder()` | New options no longer break callers (#64) |

### Changed behaviour

- **`RecordBackpressureConfig.disabled()` turns off only the record-count sensor.** In 0.2.0, disabling `BackpressureConfig` turned off *all* backpressure, including byte, heap and custom sensors. Now each sensor is configured on its own.
- **`recordsSkipped` counts only records the lifecycle hook skipped** (#64). Dropped records are counted only in `recordsDropped`. Alert on `recordsDropped` for data loss.
- **Shutdown runs every step** even if one fails with an `Error` (for example from a sink's `close()`), so the consumer is always closed and `awaitShutdown()` always returns.
- **`module-info` declares `requires transitive kafka.clients`.** Modular applications no longer need to require it themselves.

## New features

- **Sink SPI: `processor(...).sink(...)`.** A `RecordProcessor` turns each record into outputs (it may block on I/O); a `RecordSink` writes them. A record is acked, and so becomes committable, only once its outputs are durable. A fire-and-forget `producer.send()` can no longer get a record committed before its output lands. See the README section "Writing output: processor and sink".
  - **Ordering through the write:** with `ordering(KEY)` the next record of a key starts only after the previous record's write completed; `PARTITION` does the same per partition.
  - **Write failures are classified** with the new `WriteFailure` (`RecordSink.classify`, default `WriteFailure.of` for Kafka errors). `FATAL` (a missing ACL on the output topic, a closed producer) stops the pipeline with the record unacked. `TRANSIENT` (a broker timeout) is retried until the destination recovers, so an outage becomes backpressure instead of dropped records; past `writeOutageTimeout` (new, default 5 minutes) the pipeline stops, still without dropping. `RECORD` is retried up to `maxRetries`, then goes to the DLQ or is dropped. Only the write is retried, never the processor.
  - **The DLQ is classified the same way** (`DLQHandler.classify`, new default method): a DLQ that is briefly down is retried instead of dropping the record, and a fatal DLQ error stops the pipeline. This applies to handler pipelines too.
  - **Writes of a revoked partition are abandoned**, not retried or dead-lettered, so an old assignment never rewrites a record the new owner is processing.
  - **Shutdown waits for writes** within `shutdownTimeout`; sinks are flushed, and writes that finish in time are committed.
  - **Downstream pressure is global:** a sink can report pressure through `sensor()`.
- **`KafkaRecordSink`:** one shared, idempotent producer (`acks=all`), acks on the broker's callback, a `kp-source` header with the input record's id for deduplication, and a pending-sends backpressure sensor (`maxPendingSends`). Flushes at shutdown and while a partition is being revoked.
- **`KafkaDLQHandler`:** writes dead letters to a topic with the original key, value and headers plus `kp-dlq-*` headers describing the error. Can share the output sink's producer.
- **`RecordSink.blocking(...)`** wraps blocking write code.
- **`BackpressureSensor.watermark(name, count, high, low)`** builds a sensor over any count with the same hysteresis as the built-in sensors.
- **`KafkaHeaders`** names the headers the Kafka sinks add.
- **`MetricsExporter` SPI** (#64): publish metrics to any monitoring system with `metricsExporter(...)`; `PartitionMetrics` adds `windowSize`, `windowCapacity` and `windowFill`; `handlerDuration()` and `name()` are new.

## Fixes

- Retry backoffs (processor retries, write retries, DLQ retries) end at once when the pipeline starts stopping or the record's partition is revoked, instead of holding shutdown or a worker for up to `maxBackoff`. A record of a revoked partition is no longer dead-lettered or dropped by the old owner.
- `ProducerFencedException` is treated as fatal and stops the pipeline.
- `KafkaDLQHandler` reports a send the broker did not acknowledge within `sendTimeout` as Kafka's `TimeoutException` (transient), not `java.util.concurrent.TimeoutException`.

- The heap backpressure sensor no longer gives up on after-GC readings after one transient error, and logs once when it falls back to current usage.
- `HeapBackpressureConfig` rejects `NaN` watermarks.
- `KafkaRecordSink` validates its settings before creating its producer, so a bad setting does not leak producer threads.
- `RecordSink.blocking(...)` keeps the thread's interrupt flag.

## Docs

- New `CONTRIBUTING.md` (build and test, the invariants reviewers check, vocabulary and naming rules) and `ARCHITECTURE.md` (threads, a record's path, offsets, pausing, failures, rebalances, shutdown).
- Every package has a `package-info.java` describing its role, its threads and whether it is public.
- Internal packages were reorganized for readability: `consumer` (poll loop, pause controller, rebalance listener), `commit` (commit strategy), `internal.backpressure`. `UnorderedOffsetTracker` is now `SlidingWindowOffsetTracker`.
