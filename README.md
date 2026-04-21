# kafka-pipeline

[![Build](https://github.com/openlogiclab/kafka-pipeline/actions/workflows/gradle.yml/badge.svg)](https://github.com/openlogiclab/kafka-pipeline/actions/workflows/gradle.yml)
[![codecov](https://codecov.io/gh/openlogiclab/kafka-pipeline/branch/main/graph/badge.svg)](https://codecov.io/gh/openlogiclab/kafka-pipeline)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A lightweight Java library that wraps `KafkaConsumer` and handles the tedious parts — concurrent processing, backpressure, offset tracking, error handling — so you can focus on what matters: your record processing logic.

Zero additional dependencies beyond `kafka-clients` itself.

## Why

The Kafka consumer API is single-threaded by design. Scaling throughput means you're writing the same boilerplate every time: thread pools, partition-aware queuing, offset bookkeeping that doesn't lose data, graceful shutdown, rebalance handling, retry loops, dead letter queues...

This library packages all of that into a clean pipeline that you configure once and forget about. You give it a `RecordHandler`, pick your thread model, set your error policy, and it takes care of the rest.

## Design Guarantees

> **At-least-once delivery.** This library guarantees that every record is delivered to your handler at least once. It will **never** silently drop data. The tradeoff: duplicate delivery is possible after a crash (see [The Duplicate Processing Problem](#the-duplicate-processing-problem)). If you need exactly-once, implement idempotent processing in your handler.
>
> **Blocking under pressure, not dropping.** When internal queues fill up (misconfigured backpressure, unexpected burst, slow downstream), the poll thread **blocks** until space is available rather than discarding records. This preserves data integrity at the cost of temporary latency. A `WARNING` log is emitted when blocking occurs — if you see it in production, tune your backpressure thresholds or concurrency. See [Tuning `dispatchQueueCapacity`](#tuning-dispatchqueuecapacity) for details.

## Installation

**Gradle (Kotlin DSL)**
```kotlin
implementation("io.github.openlogiclab:kafka-pipeline:0.1.0-beta.1")
```

**Gradle (Groovy)**
```groovy
implementation 'io.github.openlogiclab:kafka-pipeline:0.1.0-beta.1'
```

**Maven**
```xml
<dependency>
    <groupId>io.github.openlogiclab</groupId>
    <artifactId>kafka-pipeline</artifactId>
    <version>0.1.0-beta.1</version>
</dependency>
```

## Requirements

- **Java 21+** (virtual threads support)
- **Kafka clients 4.2.0+**

## Getting Started

```java
Properties kafkaProps = new Properties();
kafkaProps.put("bootstrap.servers", "localhost:9092");
kafkaProps.put("group.id", "my-consumer-group");
kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

KafkaPipeline<String, String> pipeline = new KafkaPipeline<>(
    KafkaPipeline.<String, String>builder()
        .consumerProperties(kafkaProps)
        .topics("orders", "payments")
        .handler(record -> orderService.process(record))
        .concurrency(ThreadMode.VIRTUAL, 200)
        .build());

pipeline.start();  // blocks until stop() is called or SIGTERM
```

That's it. 200 virtual threads processing records concurrently across all assigned partitions, with automatic offset commits, backpressure, and graceful shutdown out of the box.

## How It Works

```
KafkaConsumer.poll()              single-threaded, respects backpressure
       │
       ▼
RecordDispatcher                  per-partition bounded queues
       │
 ┌─────┼─────┐
 ▼     ▼     ▼
Worker Worker Worker              platform or virtual threads
       │
       ▼
OffsetTracker → PeriodicCommitter sliding-window offset tracking, async commits
```

1. A single poll loop fetches records and registers each offset with the tracker.
2. Records are dispatched to per-partition queues (partition ordering preserved within each queue).
3. Workers pull from queues and run your handler. Processing across partitions (and within a partition) is concurrent and unordered.
4. As records complete, a sliding-window tracker figures out which offsets are safe to commit — only contiguous completed offsets advance the commit position, so you never skip over in-flight work.
5. A periodic committer pushes offsets to Kafka asynchronously, with a final sync commit on shutdown and rebalance.

## Data Flow

A concrete walkthrough of what happens to a single batch of records, from poll to commit.

### Happy Path

```
 Kafka Broker
     │
     │  poll() returns 5 records: offsets [10, 11, 12, 13, 14] on partition-0
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Poll Loop (single thread)                                           │
│                                                                     │
│  for each record:                                                   │
│    1. offsetTracker.register(tp, offset)   → state: REGISTERED      │
│    2. sensor.recordRegistered()            → in-flight count +1     │
│    3. dispatcher.dispatch(record)          → enqueue to partition-0 │
└─────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ RecordDispatcher                                                    │
│                                                                     │
│  partition-0 queue: [10, 11, 12, 13, 14]                            │
│  partition-1 queue: [...]                                           │
│                                                                     │
│  Workers parked via LockSupport get unparked on dispatch            │
└─────────────────────────────────────────────────────────────────────┘
     │
     ├──────────────┬──────────────┐
     ▼              ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│ Worker 1 │ │ Worker 2 │ │ Worker 3 │    (concurrent, unordered)
│ picks 10 │ │ picks 11 │ │ picks 12 │
└──────────┘ └──────────┘ └──────────┘
     │              │              │
     │   Each worker runs:
     │   1. offsetTracker.markInProgress(tp, offset)  → IN_PROGRESS
     │   2. hook.beforeProcess(record, ctx)            → skip if false
     │   3. handler.handle(record)                     → YOUR CODE
     │   4. hook.afterProcess(record, ctx)
     │   5. offsetTracker.ack(tp, offset)              → DONE
     │   6. sensor.recordCompleted()                   → in-flight count -1
     │
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ OffsetTracker (UnorderedOffsetTracker / PartitionWindow)            │
│                                                                     │
│  Suppose workers finish in order: 10, 12, 11, 14, 13                │
│                                                                     │
│  After 10 completes:  committable = 11  (10 is done, contiguous)    │
│  After 12 completes:  committable = 11  (gap at 11)                 │
│  After 11 completes:  committable = 13  (10,11,12 contiguous now)   │
│  After 14 completes:  committable = 13  (gap at 13)                 │
│  After 13 completes:  committable = 15  (all done, 10–14 complete)  │
│                                                                     │
│  Key: committable offset = first_incomplete_offset                  │
│       (Kafka semantics: commit N = "start from N on next poll")     │
└─────────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ PeriodicCommitter (every 5s by default)                             │
│                                                                     │
│  Reads committable offsets from tracker → commitAsync to Kafka      │
│  On shutdown/rebalance → commitSync (blocking, guaranteed)          │
└─────────────────────────────────────────────────────────────────────┘
```

### Error Path

When `handler.handle()` throws:

```
handler.handle(record) throws
     │
     ▼
Retry loop (attempt 1, 2, ... maxRetries)
  backoff: 1s → 2s → 4s → ... (exponential, capped at maxBackoff)
  hook.onError() called on each failure
     │
     ├── retry succeeds → ack, done
     │
     ▼  all retries exhausted
DLQ handler (if configured)
     │
     ├── DLQ send succeeds → ack, done (offset advances, record is "handled")
     │
     ▼  DLQ fails or not configured
Fallback
     │
     ├── SKIP          → ack anyway, log warning, move on
     │
     └── FAIL_PARTITION → mark offset FAILED, halt partition processing
```

### Rebalance Flow

When Kafka triggers a consumer group rebalance:

```
onPartitionsRevoked([partition-0, partition-2])
     │
     ▼
  1. dispatcher.removePartition(tp)
     → clear queued records, adjust sensor count
     
  2. offsetTracker.drainPartition(tp, drainTimeout)
     → wait for in-flight workers to finish (up to 30s default)
     → returns: {completed: 47, abandoned: 2, timedOut: true/false}
     
  3. committer.commitSync()
     → final offset commit for revoked partitions
     
  4. offsetTracker.clearPartition(tp)
     → remove all state for this partition

onPartitionsAssigned([partition-0, partition-5])
     │
     ▼
  1. consumer.position(tp) → get starting offset from Kafka
  2. offsetTracker.initPartition(tp, startOffset)
  3. dispatcher.addPartition(tp) → create fresh queue
  4. poll loop resumes → records flow into the new partitions
```

### Backpressure Flow

```
Poll loop evaluates all sensors before each poll:

  BackpressureController.evaluate():
    for each sensor in [record-count, byte-size, ...custom]:
      status = sensor.currentStatus()
      worst  = max(worst, status)

  Record-count sensor:
    in-flight < highWatermark           → OK
    in-flight >= highWatermark          → THROTTLE
    in-flight >= criticalThreshold      → CRITICAL

  Byte-size sensor (if enabled):
    inFlightBytes < highWatermarkBytes  → OK
    inFlightBytes >= highWatermarkBytes → THROTTLE
    inFlightBytes >= criticalBytes      → CRITICAL

  Action based on worst status:
    OK       → poll normally
    THROTTLE → consumer.pause(assignment)  → heartbeats only
    CRITICAL → consumer.pause(assignment)  → heartbeats only

  Once paused, workers keep draining...
    
  When ALL sensors return OK           → consumer.resume(assignment)
  (each sensor has its own low watermark hysteresis)
```

The high/low watermark gap is the hysteresis band. Without it, the consumer would thrash between pause and resume on every single record completing near the threshold.

## Configuration

### Thread Model

Two options, set explicitly when you configure concurrency:

```java
// Fixed pool of OS threads — good for CPU-bound work
.concurrency(ThreadMode.PLATFORM, 16)

// Virtual threads (JDK 21+) — good for I/O-bound work (DB, HTTP, etc.)
.concurrency(ThreadMode.VIRTUAL, 500)
```

### Error Handling

The pipeline runs a full error chain on every failure: **retry → DLQ → fallback**.

```java
.errorStrategy(ErrorStrategy.<String, String>builder()
    .maxRetries(3)
    .retryBackoff(Duration.ofSeconds(1))
    .exponentialBackoff(true)
    .maxBackoff(Duration.ofSeconds(30))
    .dlqHandler((record, error) -> dlqProducer.send(toDlqRecord(record, error)))
    .fallback(Fallback.SKIP)     // or Fallback.FAIL_PARTITION
    .build())
```

If all retries fail, the DLQ handler gets a shot. If that also fails (or isn't configured), the `fallback` decides: skip the record and move on, or halt processing for that partition.

For simple cases there are factory methods:

```java
ErrorStrategy.failFast()    // no retries, fail partition immediately
ErrorStrategy.skipOnError() // no retries, skip and move on
```

### Batch Mode

For high-throughput scenarios where per-record overhead matters — bulk inserts, file aggregation, batched API calls — use `batchHandler` instead of `handler`. Records from each partition are delivered as a single batch, and offset tracking uses one lock acquisition per batch instead of per record.

```java
KafkaPipeline<String, String> pipeline = new KafkaPipeline<>(
    KafkaPipeline.<String, String>builder()
        .consumerProperties(kafkaProps)
        .topics("events")
        .batchHandler((partition, records) -> {
            bulkInsert(records);
        })
        .concurrency(ThreadMode.VIRTUAL, 50)
        .build());
```

Batch mode uses the same error chain (retry → DLQ → fallback), but applied to the entire batch. The sliding window still tracks out-of-order batch completion correctly — if batch A finishes before batch B, the commit offset won't advance past batch B's range until it completes.

You must use either `handler` (per-record) or `batchHandler` (batch), not both.

### Backpressure

The pipeline supports multiple backpressure layers that work together. The poll loop checks all active sensors and applies the worst status.

#### Record-Count Backpressure

Enabled by default. Uses high/low watermark hysteresis to prevent pause/resume flapping:

```java
.backpressure(BackpressureConfig.builder()
    .highWatermark(10_000)     // pause polling when in-flight records reach this
    .lowWatermark(6_000)       // resume polling when count drops to this
    .criticalThreshold(50_000) // hard stop
    .build())
```

#### Byte-Level Backpressure

Protects against memory pressure when individual records are large (e.g. 10 MB file payloads). Tracks total in-flight payload bytes using `ConsumerRecord.serializedKeySize() + serializedValueSize()`. Disabled by default — opt in with:

```java
.byteBackpressure(ByteBackpressureConfig.builder()
    .lowWatermarkBytes(128 * 1024 * 1024)     // 128 MB — resume
    .highWatermarkBytes(256 * 1024 * 1024)    // 256 MB — pause
    .criticalThresholdBytes(512 * 1024 * 1024) // 512 MB — hard stop
    .build())
```

Both sensors run in parallel. If record count is fine but byte usage crosses the threshold (or vice versa), the consumer pauses until both recover.

#### Custom Backpressure Sensors

You can plug in your own sensors for domain-specific pressure signals — external queue depth, downstream service health, JVM heap usage, anything:

```java
.addBackpressureSensor(new BackpressureSensor() {
    @Override
    public BackpressureStatus currentStatus() {
        double heapUsage = Runtime.getRuntime().totalMemory()
            / (double) Runtime.getRuntime().maxMemory();
        if (heapUsage > 0.9) return BackpressureStatus.CRITICAL;
        if (heapUsage > 0.7) return BackpressureStatus.THROTTLE;
        return BackpressureStatus.OK;
    }

    @Override
    public String name() { return "heap-pressure"; }

    @Override
    public String statusDetail() { return "custom heap monitor"; }
})
```

The pipeline evaluates all sensors (record-count, byte-level, and any custom ones) on each poll cycle and uses the worst status across all of them.

When backpressure kicks in, the consumer is paused (not disconnected) — it stays in the group and responds to heartbeats, but stops fetching new records until workers catch up.

### Lifecycle Hooks

For cross-cutting concerns — deduplication, metrics, tracing — without cluttering your handler:

```java
.lifecycleHook(new ProcessingLifecycleHook<>() {
    @Override
    public boolean beforeProcess(ConsumerRecord<String, String> record, ProcessingContext ctx) {
        // Return false to skip (e.g., dedup check)
        return !dedupStore.exists(ctx.topic(), ctx.partition(), ctx.offset());
    }

    @Override
    public void afterProcess(ConsumerRecord<String, String> record, ProcessingContext ctx) {
        dedupStore.mark(ctx.topic(), ctx.partition(), ctx.offset());
        metrics.recordProcessed(ctx.topic());
    }

    @Override
    public void onError(ConsumerRecord<String, String> record, ProcessingContext ctx, Exception error) {
        metrics.recordFailed(ctx.topic(), error);
    }
})
```

`beforeProcess` returning `false` is the idempotency hook — the record gets acked without touching your handler.

### Async Start

If you don't want to block the main thread:

```java
KafkaPipeline<String, String> pipeline = new KafkaPipeline<>(config);
Thread t = pipeline.startAsync();

// ... do other stuff ...

pipeline.stop();
pipeline.awaitShutdown();
```

### All Config Options

| Option | Default | Description |
|---|---|---|
| `consumerProperties` | *required* | Standard Kafka consumer properties (`enable.auto.commit` is forced to `false`) |
| `topics` | *required* | Topics to subscribe to |
| `handler` | *one required* | Your `RecordHandler<K,V>` for per-record mode |
| `batchHandler` | *one required* | Your `BatchRecordHandler<K,V>` for batch mode |
| `concurrency(ThreadMode, int)` | *required* | Thread model and worker count |
| `backpressure` | high=10k, low=6k, critical=50k | Record-count watermarks |
| `byteBackpressure` | disabled | Byte-level watermarks for memory protection |
| `addBackpressureSensor` | none | Custom backpressure sensor(s) |
| `errorStrategy` | fail-fast | Retry + DLQ + fallback chain |
| `lifecycleHook` | no-op | Before/after/error callbacks (per-record mode only) |
| `commitInterval` | 5s | How often to commit offsets async |
| `drainTimeout` | 30s | Max wait for in-flight records during rebalance |
| `shutdownTimeout` | 30s | Max wait for workers during shutdown |
| `pollTimeout` | 100ms | Kafka consumer poll timeout |
| `dispatchQueueCapacity` | 500 | Bounded queue capacity for pending work (see below) |

### Tuning `dispatchQueueCapacity`

This controls how much pending work can queue up between the poll loop and worker threads:

- **Per-record mode** — capacity of each per-partition dispatch queue.
- **Batch mode** — capacity of the executor's task queue (PLATFORM threads only; virtual threads create a lightweight thread per task and don't queue).

The default is **500**, matching Kafka's default `max.poll.records`. A good rule of thumb: set it to your `max.poll.records` value.

```java
// If you set max.poll.records=1000, match it:
kafkaProps.put("max.poll.records", "1000");

KafkaPipeline.<String, String>builder()
    .consumerProperties(kafkaProps)
    .topics("events")
    .handler(record -> process(record))
    .concurrency(ThreadMode.VIRTUAL, 200)
    .dispatchQueueCapacity(1000)
    .build();
```

When the queue fills up, the dispatch call **blocks** the poll thread until space is available. This acts as a runtime safety net — you'll see a `WARNING` log if it happens:

```
WARNING: Dispatch queue full for topic-0, blocking until space is available
         — review backpressure thresholds vs dispatchQueueCapacity
```

> **Warning:** If you see this message in production, it typically means backpressure thresholds are set too high relative to processing speed, or `dispatchQueueCapacity` is too low. Fix the root cause — don't just increase the queue capacity, as oversized queues waste memory and reduce backpressure sensitivity (records sit idle in the queue longer before backpressure kicks in).

A full example tying `dispatchQueueCapacity`, `max.poll.records`, and backpressure together:

```java
// max.poll.records = 500 (default)
// backpressure highWatermark = 2000  (pause after ~4 polls worth in-flight)
// dispatchQueueCapacity = 500        (one poll's worth per partition queue)

KafkaPipeline.<String, String>builder()
    .consumerProperties(kafkaProps)
    .topics("orders")
    .handler(record -> orderService.process(record))
    .concurrency(ThreadMode.VIRTUAL, 100)
    .backpressure(BackpressureConfig.builder()
        .highWatermark(2000)
        .lowWatermark(1000)
        .criticalThreshold(10_000)
        .build())
    .dispatchQueueCapacity(500)
    .build();
```

In this setup, backpressure pauses the poll loop well before the dispatch queue fills up. The queue only blocks as a last resort if something unexpected happens at runtime.

## Offset Management

This is the hardest part to get right, and the main reason this library exists.

With concurrent processing, records complete out of order. Naively committing the highest completed offset would lose data — if the process crashes, any in-flight record before that offset is silently skipped.

### Sliding Window

The offset tracker maintains a per-partition sliding window (similar to TCP's receive window) that tracks the state of every in-flight offset:

```
partition-0 window:

  offset:  10    11    12    13    14    15    16
  state:  [DONE] [DONE] [DONE] [IN_PROGRESS] [DONE] [DONE] [IN_PROGRESS]
                         ▲                                         ▲
                  committable = 13               window right edge = 16
                  (first incomplete)
```

The committable offset only advances through **contiguous** completed records. In the example above, offsets 10–12 are done, so the safe commit position is 13. Even though 14 and 15 are also done, committing 16 would be wrong — if the process crashes before 13 finishes, it would never be reprocessed.

As workers complete records, the left edge of the window slides forward:

```
  13 completes → committable jumps to 16  (13, 14, 15 all contiguous now)
  16 completes → committable = 17         (window fully drained)
```

This gives you **at-least-once delivery** guarantees with fully concurrent, unordered processing. The window is bounded — when it fills up, backpressure kicks in and the poll loop pauses until workers catch up.

On rebalance, the pipeline drains in-flight records (up to `drainTimeout`), does a final sync commit, then hands the partition off cleanly.

### The Duplicate Processing Problem

The sliding window guarantees that the committed offset never jumps past in-flight work. But there's one scenario it **cannot** prevent:

```
  offset:  1     2     3     4     5
  state:  [DONE] [DONE] [IN_PROGRESS] [DONE] [IN_PROGRESS]
                         ▲
                  committable = 3

  → System crashes. Offsets 1 and 2 were committed. 3, 4, 5 were not.
  → New consumer starts from offset 3.
  → Offset 4 was already processed successfully — it gets processed again.
```

This is inherent to at-least-once delivery with concurrent processing. The library commits conservatively (only contiguous completions), which minimizes the blast radius — but any completed record sitting behind an in-flight record will be redelivered after a crash.

No amount of offset tracking can fix this. The only real solution is **idempotent processing** on the consumer side.

The library doesn't try to solve idempotency for you — that's deeply tied to your downstream system. Instead, it gives you the `ProcessingLifecycleHook` as an extension point. Use `beforeProcess` to check a dedup store, `afterProcess` to mark completion. Common strategies:

- DB upsert (`INSERT ... ON CONFLICT DO UPDATE`)
- Dedup table keyed by `topic:partition:offset`
- Conditional writes with version or timestamp guards

## Building

```bash
./gradlew build
```

## Running Tests

```bash
./gradlew test
```

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes before submitting a pull request.

## Disclaimer

This software is provided "as is", without warranty of any kind. See the [LICENSE](LICENSE) for full terms.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
