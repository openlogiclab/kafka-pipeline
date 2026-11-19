# Architecture

This is the map for contributors: what the parts are, which thread runs what, and how a record moves through the pipeline. For how to use the library, see the [README](README.md). For the reasoning behind the sink and exactly-once work, see [docs/design](docs/design).

Everything below is in the `kafka-pipeline` module, which has everything Kafka: besides the pipeline, the Kafka sink (`sink.kafka.KafkaRecordSink`) and the Kafka DLQ (`error.KafkaDLQHandler`). The pipeline sees the Kafka sink only through the `RecordSink` SPI, like any other sink. Sinks for other destinations, which bring their own dependencies, go in separate modules.

## The idea

Kafka orders records within a partition, and a plain consumer processes them one at a time. When processing waits on I/O, throughput is capped at *partitions ÷ per-record latency*. kafka-pipeline separates concurrency from the partition count: many workers process records of the same partition at once, ordering is kept only where you ask for it (per key or per partition), and offsets are committed only past records that are finished. Backpressure is global, so a slow downstream pauses fetching for everyone instead of piling up memory.

## Threads

| Thread | What runs on it | Owns |
|---|---|---|
| **Poll thread** (one) | `consumer.PollLoop`: commit when due, evaluate backpressure, pause/resume partitions, poll, hand records to the worker pool. Rebalance callbacks (`consumer.PipelineRebalanceListener`). Commits (`commit.PeriodicCommitter`). | The `KafkaConsumer`. Nothing else may touch it except `wakeup()`. |
| **Workers** (`concurrency` of them, platform or virtual) | `worker.SingleRecordWorkerPool` or `worker.BatchWorkerPool`: run the handler or processor, with retries and the processing timeout. | Nothing shared; they go through thread-safe components. |
| **Sink write outcomes** (virtual) | `worker.SinkWrites`: when a sink write completes, ack the record, retry a failed write, or send it to the DLQ. | Nothing; never runs on the sink's own I/O thread. |
| **Timeout timer** (one, only if `processingTimeout` is set) | `worker.AttemptTimer`: interrupts handler attempts that run too long. | |
| **Drain threads** (virtual, during a revoke) | Wait for in-flight records of revoked partitions, up to `drainTimeout`. | |

## A record's path (per-record mode)

```
poll thread                                     worker                              sink I/O → outcome thread
───────────                                     ──────                              ─────────────────────────
consumer.poll()
  │ register offset in the partition's window   (offset.SlidingWindowOffsetTracker)
  │ count it in flight                          (internal.backpressure.InFlightCounter)
  ▼
RecordDispatcher.dispatch → lane of its partition
                                                RecordDispatcher.poll
                                                  (respects KEY / PARTITION order and
                                                   maxConcurrencyPerPartition)
                                                markInProgress
                                                processor / handler   (retries, timeout)
                                                  ├─ Optional.empty() ──► ack, release
                                                  ├─ an output ─────────► sink.write ──► stage completes
                                                  │                                       ├─ ok → ack, release
                                                  │                                       └─ fail → retry write → DLQ / drop → ack, release
                                                  └─ failed ────────────► DLQ / drop → ack, release
PeriodicCommitter (every commitInterval)
  commits each partition up to its first unfinished offset
```

*Release* means freeing the record's in-flight slot (global backpressure) and its dispatcher slot (ordering and the per-partition limit). It happens exactly once per record, in `SingleRecordWorkerPool.Completion`.

Batch mode skips the dispatcher: `BatchWorkerPool` submits one task per partition per poll and acks the batch's offsets together.

## Offsets: one window per partition

`offset.PartitionWindow` holds the state of each polled offset (`REGISTERED → IN_PROGRESS → DONE`). Records finish in any order, so the window accepts acks in any order; the committable offset only moves past a contiguous run of `DONE` offsets. After every poll, the poll thread also tells the window where the consumer fetches next; once every registered record is done, the committable offset moves up to that position, past offsets that are not records (transaction markers, aborted or compacted-away offsets). A stuck record therefore holds back its partition's commit, never another partition's, and never lets an unfinished record be committed.

With `skipCompletedOnRestart` (on by default), each commit also stores, in the commit metadata, which offsets after the committed one are already done (`offset.CompletedOffsetsCodec`). The next owner of the partition skips those records instead of processing them again.

The rest of the code sees the tracker only through narrow role interfaces: `ProcessingProgress` (workers), `CommitSource` (committer), `PartitionLifecycle` (rebalance listener) and `OffsetStats` (metrics and pausing).

## Pausing: global throttle and per-partition holds

Before every poll, `PartitionPauseController.apply` decides for each assigned partition whether to fetch from it. A partition is paused if **any** of these is true:

- **Global throttle**: any backpressure sensor says THROTTLE (`internal.backpressure.BackpressureController`). Sensors: records in flight, bytes in flight, heap, the sink's pending writes, and custom sensors. The built-in ones share one hysteresis rule (`WatermarkSensor`); custom sensors decide for themselves.
- **Window nearly full**: the partition's window reached 80% of `maxWindowSize` (resumes at 50%).
- **Records sent back**: its queue or window had no room, so the consumer sought back and paused it until there is room.
- **Holds**: the partition is not initialized yet, or batch mode has its limit of batches in flight.

The global throttle relieves pressure. The others are limits of one partition and never stand in for it.

## Failures

`worker.RetryExecutor` retries a processing attempt with the error strategy's backoff. Sink write and DLQ failures are classified first (`WriteFailure`, from `RecordSink.classify` / `DLQHandler.classify`): `FATAL` stops the pipeline with the record unacked, `TRANSIENT` is retried until the destination recovers (an outage becomes backpressure; after `writeOutageTimeout` the pipeline stops), and only `RECORD` failures use the retry budget and can end in a drop. Retry backoffs end at once when the pipeline starts stopping. When the retries are used up, `worker.FailureResolver` sends the record to the DLQ or drops it (`FinalFailureHandler` first), and acks it either way, so the partition keeps moving. A `classify` that returns null or throws counts as `RECORD`, and every write outcome is reported exactly once, so a broken classifier can neither hang a record nor stop the pipeline. Errors that retrying cannot fix (authentication, authorization, fencing, `OutOfMemoryError`; see `internal.FatalErrors`) stop the pipeline; `start()` rethrows them and `failure()` returns them.

## Rebalances

- **Revoked**: drop queued records of the partition, tell the sink (`onPartitionRevoked`), wait up to `drainTimeout` for in-flight ones, commit, forget the partition. Late acks from records still running are discarded by a generation check.
- **Lost** (this consumer was removed from the group): drop and forget without committing.
- **Assigned**: create the window at the consumer's position, reading the committed metadata to skip records already done. If the group has no committed offset there (a new group, or a reset or seek), the first commit writes that position even before any record finishes, so a later owner resumes there instead of applying `auto.offset.reset` and skipping records. A partition that fails to initialize stays paused and is retried.

## Shutdown

`stop()` (or SIGTERM) ends the poll loop. Then: workers stop (records already running get `shutdownTimeout`), sink writes still in flight get the rest of that time, the final commit runs, the processor and sink are closed, and the consumer is closed. Records that were queued or not finished are not committed and are redelivered on the next start.
