# Contributing to kafka-pipeline

Thanks for helping. This guide covers what you need to make a change that is easy to review: how to build and test, how the code is laid out, and the rules that keep it correct and readable.

Please open an issue to discuss a change before sending a pull request, unless it is a small fix.

## Build and test

You need JDK 21. Docker is only needed for the integration tests.

```bash
./gradlew spotlessApply                    # format (google-java-format) and add license headers
./gradlew test                             # unit tests, about a minute
./gradlew integrationTest                  # against a real Kafka broker in Docker (Testcontainers)
./gradlew build                            # what CI runs: format check, unit and integration tests
./gradlew jacocoTestCoverageVerification   # coverage rule: at least 70% of instructions covered
./gradlew javadoc                          # public API docs; keep it free of errors and warnings
```

Run `./gradlew spotlessApply build jacocoTestCoverageVerification javadoc` before you push.

Integration tests are skipped when Docker is not available, so a green `check` on a machine without Docker has not run them.

## Where things are

Read [ARCHITECTURE.md](ARCHITECTURE.md) first. In short:

| Package | Role | Threads | Public? |
|---|---|---|---|
| root | `KafkaPipeline`, `PipelineConfig` and its builder, `ProcessingOrder`, `ThreadMode` | any | yes |
| `handler` | `RecordHandler`, `BatchRecordHandler`, `ProcessingLifecycleHook` | workers | yes |
| `sink`, `sink.kafka` | `RecordProcessor`, `RecordSink`, `SinkContext`; `KafkaRecordSink`, `KafkaDLQHandler`, `KafkaHeaders` | workers, sink I/O | yes |
| `error` | `ErrorStrategy`, `Fallback`, `DLQHandler`, `FinalFailureHandler` | workers | yes |
| `backpressure` | settings records and the `BackpressureSensor` SPI | poll thread | yes |
| `metrics` | `PipelineMetrics`, `PartitionMetrics`, `MetricsExporter` SPI | any | yes |
| `consumer` | `PollLoop`, `PartitionPauseController`, `PipelineRebalanceListener` | poll thread only | no |
| `commit` | `CommitStrategy`, `PeriodicCommitter` | poll thread only | no |
| `dispatch` | `RecordDispatcher` and its lanes (`FIFOLaneQueue`, `KeyedLaneQueue`) | poll thread adds, workers take | no |
| `worker` | worker pools, retries, processing timeout, sink writes | workers | no |
| `offset` | `SlidingWindowOffsetTracker`, `PartitionWindow`, role interfaces | any (thread-safe) | no |
| `internal`, `internal.backpressure` | metrics collector, fatal errors, built-in sensors, controller | various | no |

Public packages are exported from `module-info.java`; everything else is invisible to users and can change freely. Every package has a `package-info.java` with its role and threads; keep it current when you add a class.

## Rules that keep the pipeline correct

These are the invariants reviewers check first. A change that breaks one is a bug even if tests pass.

1. **One thread owns the consumer.** Only the poll thread calls `KafkaConsumer` (everything in `consumer` and `commit`), except `wakeup()`. Rebalance callbacks run on the poll thread too.
2. **Offsets are per partition, backpressure is global.** Each partition has its own window and commits independently. Backpressure (in-flight records, bytes, heap, sink pressure, custom sensors) pauses *every* partition: pausing one does not relieve pressure on shared workers or memory. Per-partition pauses exist only for per-partition limits (window nearly full, queue full, ordering, batch limit, partition not initialized) and are combined with the global throttle in `PartitionPauseController.apply`.
3. **Every polled record is finished exactly once.** Its in-flight slot (`InFlightCounter.completed`) and its dispatcher slot (`RecordDispatcher.release`) are released exactly once on every path: success, failure, drop, shutdown, revoke, late completion. Look at `SingleRecordWorkerPool.Completion`.
4. **Nothing unfinished is committed.** A record is acked only when it is done: processed, its sink write durable, dead-lettered or dropped. A window commits only past a contiguous run of finished offsets.
5. **A bad record never blocks its partition.** After retries it goes to the DLQ or is dropped, and is acked either way. Only shutdown, a revoke, or a JVM-fatal error leaves a record unacked, and then it is redelivered.
6. **Interrupts mean shutdown only while the pool is stopping.** `stop()` sets the running flag to false before interrupting. An interrupt a handler leaves behind while running is cleared and treated as an ordinary failure.
7. **Never block the poll thread** on worker progress (full queues are handled by seeking back and pausing), and never do blocking work on a sink's I/O thread (write outcomes run on pipeline virtual threads).

## Naming and readability

The library is read far more often than it is written. Prefer the clear name over the short one.

- **One word per concept, one concept per word.** Use the project's vocabulary:
  - *processed*: the handler or processor succeeded (and the sink write, if any, is durable).
  - *skipped*: deliberately not processed: the lifecycle hook said no, or the record already finished before a restart (`skipCompletedOnRestart`).
  - *dropped*: given up on after the error strategy's retries without reaching a DLQ (none configured, `Fallback.DROP`, or the DLQ send failed); the `FinalFailureHandler` saw it.
  - *dead-lettered*: sent to the DLQ.
  - *in flight*: polled and not finished yet (counts for backpressure).
  - *committable*: every offset before it is finished.
  - *revoked* / *lost*: a partition leaving this consumer normally / without a chance to commit.
- **Names say what something is, not one detail of it.** For example, the offset tracker is `SlidingWindowOffsetTracker`, not "UnorderedOffsetTracker", because "unordered" read as if it were tied to `ProcessingOrder.UNORDERED`.
- **Acronyms are written in capitals** in class names, as in the existing `DLQHandler`, `KafkaDLQHandler` and `FIFOLaneQueue`.
- **Units go in the name when a value is not a count.** `ByteBackpressureConfig` uses `highWatermarkBytes` because its watermarks are bytes; record counts and heap ratios use plain `highWatermark`.
- **Explain numbers.** A threshold or interval gets a named constant with a Javadoc or comment saying why it has that value.
- **Say which thread.** A field touched by more than one thread says how it is guarded (`// poll thread only`, `// guarded by lock`, `volatile`).
- **Docs move with the code.** If you rename or change behaviour, update the Javadoc, the `package-info.java`, the README and `docs/design/` in the same pull request. Stale docs are treated as bugs.

## Tests

- Every bug fix comes with a test that fails without the fix.
- Concurrency tests wait for a condition (`await(() -> ..., timeout)`), never for a fixed sleep alone, and assert the invariant (committable offset, in-flight count) rather than timing.
- `MockConsumer.rebalance(...)` runs the rebalance listener on the calling thread. Wait until the pipeline has subscribed (`consumer.subscription()` is not empty) before calling it.
- Anything that needs real fetch, commit or group behaviour belongs in `src/integrationTest` (Testcontainers).

## Pull requests

- Keep a pull request to one topic. Refactors and behaviour changes go in separate commits.
- Describe what changed for users and which invariant above, if any, the change touches.
- Breaking changes to a public package are listed in the release notes. The project is pre-1.0, so they ship in the next minor version.
