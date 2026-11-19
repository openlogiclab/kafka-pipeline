# Design: sink SPI and exactly-once output

Status: phase 1 (sink SPI, at-least-once) implemented in 0.3.0; phase 2 (exactly-once) is design only, targeting 0.4.0

> **Implemented API differs from the first draft in these points:** `SinkContext` has no `generation` (it stays internal); `.sink(...)` accepts `RecordSink<? super O>`; the DLQ is configured with the existing `dlqHandler(...)` using `KafkaDLQHandler` (no new `dlqSink(...)` method, so `ErrorStrategy` is unchanged); `BackpressureSensor.watermark(name, supplier, high, low)` takes a name; write outcomes run on virtual threads owned by the pipeline, never on the sink's I/O thread; the pipeline closes the processor, the sink and a `KafkaDLQHandler`, but not a user's own `DLQHandler`; header names live in `KafkaHeaders` (`kp-source` and the `kp-dlq-*` headers). Not yet implemented: a write timeout (a sink must complete every stage), batch-mode sinks, sink metrics in `PipelineMetrics`, and subtracting a buffering sink's flush interval from `PartitionMetrics.commitStallMillis`. After `onPartitionRevoked`, a buffering sink must write that partition's later outputs at once instead of buffering them (the pipeline also calls `flush(tp)` after every write of a partition that is being drained, so batching sinks such as `KafkaRecordSink` do not hold those writes back).

## 1. Motivation

Today a pipeline ends in a user `RecordHandler`. When the handler writes somewhere (Kafka, a database, S3, HTTP), the library does not know about the write. Two problems follow.

- **Silent loss.** A handler that calls `producer.send()` and returns gets its record acked and committed before the write lands. If the write fails, the record is gone.
- **No exactly-once output.** Even a careful handler can only give at-least-once output, because the offset commit and the write are not atomic.

This design adds a sink abstraction. With it the library owns three things: when a record counts as done (after its output is durable), how slow writes feed backpressure, and, for Kafka outputs, an opt-in exactly-once mode that keeps processing concurrent.

### Positioning

This design is not an attempt to reimplement Kafka Streams. Streams' exactly-once runs one task per partition and processes one record at a time inside a transaction, so its parallelism is capped by the partition count. Here, processing stays concurrent (many workers per partition, per-key ordering only where it is asked for), and transactions bound only when results become visible. The closest peer is Confluent Parallel Consumer.

### What "exactly-once" means here

It means the same as in Streams: **a record may be processed more than once, but its output and its consumed offset are committed exactly once, atomically.** Downstream consumers using `read_committed` see each output once. Side effects outside Kafka are not covered; database, S3 and HTTP sinks get the equivalent result through idempotent writes (§4.4).

## 2. Goals and non-goals

Goals:

1. A public `RecordSink` SPI that any downstream can implement, so Kafka, JDBC, S3 and HTTP are all just implementations.
2. A record is acked only when every one of its outputs is durable.
3. Pending writes count against global backpressure. A sink can also expose its own pressure. Nothing pauses a single partition to relieve pressure.
4. Ordering (`KEY`, `PARTITION`) holds until the write completes, not just until the handler returns.
5. Write failures are classified: fatal client errors stop the pipeline, transient ones are retried until the destination recovers, and record-specific ones go through retry → DLQ → drop. A retry repeats only the write.
6. An opt-in `exactlyOnce()` mode for Kafka outputs, using one transactional producer per pipeline, with concurrent processing.

Non-goals:

- State stores, joins, windowing: no Streams-style DSL.
- Exactly-once for non-Kafka sinks: those use idempotent writes keyed by `SinkContext`.
- Distributed transactions (XA) across Kafka and a database.

## 3. Public API

### 3.1 Processor and sink

The pipeline splits the work into two steps. The **processor** is the business logic: it runs on the worker pool, may do blocking I/O (database lookups, HTTP calls, enrichment; this is where the library's concurrency pays off), and returns what should be written. The **sink** writes those outputs. Keeping the write out of the processor is what lets the library ack a record only after its output is durable, and lets exactly-once mode put the write in a transaction.

```java
package io.github.openlogiclab.kafkapipeline.sink;

/**
 * Processes one input record and returns 0..n outputs for the sink (an empty list means "nothing
 * to write"; the record is still acked). Runs concurrently on the worker pool and may block on
 * I/O. It may run more than once for the same record (retries, redelivery), so external side
 * effects in it should be idempotent; the write to the sink is what the library makes exactly-once.
 */
@FunctionalInterface
public interface RecordProcessor<K, V, O> extends AutoCloseable {
  List<O> process(ConsumerRecord<K, V> record) throws Exception;

  /** Called once when the pipeline shuts down, after the last record finished. */
  @Override
  default void close() throws Exception {}
}

/** Writes outputs downstream. Thread-safe: called concurrently by many workers. */
@FunctionalInterface
public interface RecordSink<O> extends AutoCloseable {

  /**
   * Accepts the outputs of one input record. The returned stage completes when they are
   * durable downstream, and completes exceptionally if they cannot be written. Must not block
   * for long: use an async client or hand off to your own executor.
   */
  CompletionStage<Void> write(SinkContext ctx, List<O> outputs);

  /** Pushes buffered data out for one partition (revoke, shutdown, size/time trigger). */
  default CompletionStage<Void> flush(TopicPartition tp) { return CompletableFuture.completedFuture(null); }

  /** The partition left this consumer. Flush within the budget, or drop (it is redelivered). */
  default void onPartitionRevoked(TopicPartition tp, Duration budget) {}

  /** Optional downstream pressure, added to the global backpressure controller. */
  default Optional<BackpressureSensor> sensor() { return Optional.empty(); }

  /** Largest number of records this sink may hold unacked per partition (buffering sinks). */
  default OptionalInt maxBufferedRecordsPerPartition() { return OptionalInt.empty(); }

  @Override default void close() {}

  /**
   * Wraps a blocking writer. It runs on the worker thread that processed the record and the stage
   * completes when it returns, so the worker waits for the write. Cheap with virtual threads; with
   * platform threads it ties up a worker per write in progress.
   */
  static <O> RecordSink<O> blocking(BlockingWriter<O> writer) {
    return (ctx, outputs) -> {
      try {
        writer.write(ctx, outputs);
        return CompletableFuture.completedFuture(null);
      } catch (Exception e) {
        return CompletableFuture.failedFuture(e);
      }
    };
  }

  @FunctionalInterface
  interface BlockingWriter<O> {
    void write(SinkContext ctx, List<O> outputs) throws Exception;
  }
}

/** Stable identity of the input record, for idempotent writes. */
public record SinkContext(TopicPartition partition, long offset, Headers headers) {
  /** "topic-partition-offset": a natural idempotency / upsert key. */
  public String recordId() { ... }
}
```

`RecordSink` also has a single abstract method (`write`; everything else has a default), so `.sink(...)` accepts the same three forms. Which one fits depends on the sink:

```java
// 1. Lambda over an async client: the stage completes when the client's future does
.sink((ctx, outputs) -> httpClient
    .sendAsync(toRequest(outputs, ctx.recordId()), BodyHandlers.discarding())  // idempotency key
    .thenAccept(r -> { if (r.statusCode() >= 300) throw new SinkWriteException(r.statusCode()); }))

// 2. Method reference to blocking code, wrapped with RecordSink.blocking(...)
//    void upsert(SinkContext ctx, List<EnrichedOrder> rows) throws SQLException
.sink(RecordSink.blocking(orderRepository::upsert))

// 3. Class: owns its client, buffers, flushes, reports pressure; the form for real sinks
public final class OrderIndexSink implements RecordSink<EnrichedOrder> {
  private final SearchClient client;
  OrderIndexSink(SearchClient client) { this.client = client; }

  @Override
  public CompletionStage<Void> write(SinkContext ctx, List<EnrichedOrder> outputs) {
    return client.bulkIndexAsync(outputs, ctx.recordId());   // doc id = recordId → idempotent
  }

  @Override
  public Optional<BackpressureSensor> sensor() {             // pending requests → global throttle
    // new helper: a hysteresis sensor over any counter (high 2,000, low 1,000)
    return Optional.of(BackpressureSensor.watermark("search-pending", client::pendingRequests, 2_000, 1_000));
  }

  @Override
  public void close() { client.close(); }                     // pipeline calls this on shutdown
}

.sink(new OrderIndexSink(searchClient))
```

Rules for every form:

- **`write` is called concurrently** by many workers on one instance, so the sink must be thread-safe.
- **The returned stage defines done.** Complete it only when the outputs are durable. Failing it retries the write (see §4.4) and, for a record-specific failure, ends in the DLQ or a drop. Never complete it early.
- **Never block inside a lambda that returns an incomplete stage** (form 1). If the code blocks, use `RecordSink.blocking(...)` (form 2), which makes the blocking explicit and keeps it on the worker thread.
- **Buffering, flushing, `onPartitionRevoked` and pressure sensors** need overriding defaults, so those sinks (Kafka, S3, bulk JDBC) are always classes (form 3). The first-party sinks (`KafkaRecordSink`, `KafkaDLQHandler`, the JDBC and S3 modules) are classes built with builders.
- **Exactly-once mode** only accepts `KafkaRecordSink`, because the write must be part of the transaction (§3.2).

Because `RecordProcessor` has a single abstract method, `.processor(...)` accepts any of the three usual forms:

```java
// 1. Lambda: short logic inline
.processor(record -> List.of(enrich(record.value())))

// 2. Method reference: logic in an existing method with a matching signature
//    List<EnrichedOrder> enrich(ConsumerRecord<String, Order> record) throws Exception
.processor(orderService::enrich)

// 3. Class: logic with its own dependencies, state and cleanup
public final class OrderEnrichmentProcessor
    implements RecordProcessor<String, Order, EnrichedOrder> {

  private final CustomerClient customers;
  private final PricingDao pricing;

  public OrderEnrichmentProcessor(CustomerClient customers, PricingDao pricing) {
    this.customers = customers;
    this.pricing = pricing;
  }

  @Override
  public List<EnrichedOrder> process(ConsumerRecord<String, Order> record) throws Exception {
    Order order = record.value();
    Customer customer = customers.get(order.customerId());
    return List.of(new EnrichedOrder(order, customer, pricing.priceFor(order.sku(), customer.tier())));
  }

  @Override
  public void close() {                 // pipeline calls this on shutdown
    customers.close();
    pricing.close();
  }
}

.processor(new OrderEnrichmentProcessor(customers, pricing))
```

One instance is shared by all workers, so a processor class must be thread-safe: keep per-record state in local variables, and use thread-safe clients. The class form is the one to use for unit tests (call `process` directly with a test `ConsumerRecord`) and when the processor owns resources: the pipeline calls `close()` once at shutdown, after the last record has finished and before the sinks are closed. `BatchRecordProcessor` works the same way. Today's `RecordHandler` already accepts all three forms but has no `close()`; adding the same default `close()` to it is a compatible change worth making in the same release.

The sink is optional: it is required only when there is output to write. The builder accepts two shapes and validates them at `build()`:

| Builder | Use when | Sink | Ack happens |
|---|---|---|---|
| `.handler(...)` (today's API, unchanged) | Processing is all side effects: the handler writes where it wants, or nowhere | Not allowed | When the handler returns |
| `.processor(...).sink(...)` | Processing produces output the library should write and track | Required | When the output is durable |
| `.batchHandler(...)` / `.batchProcessor(...).sink(...)` | Same, in batch mode | Same rules | Same rules |

Invalid combinations fail fast: `.processor(...)` without `.sink(...)` (outputs would have nowhere to go), `.handler(...)` with `.sink(...)` (the handler returns nothing to write), and `.exactlyOnce(...)` without a `KafkaRecordSink`. Existing pipelines need no change.

Internally there is one code path: a handler is run as a processor that returns no outputs, with a no-op sink that completes immediately. So retries, DLQ, ordering and backpressure behave the same in both shapes. The only difference is when the record counts as done. With a handler, the handler must finish its own writes before returning (for example, wait for `producer.send(...).get()`), because the library cannot see them.

Full example: an order-enrichment service. It consumes `orders`, calls a customer service over HTTP and a pricing database for each order (two blocking calls of about 20 ms), and publishes the enriched order to `orders-enriched`. Orders that keep failing go to `orders-dlq`. Methods that do not exist yet are marked `// new`; everything else is today's API.

```java
public final class OrderEnricher {

  public static void main(String[] args) throws Exception {
    String pod = System.getenv().getOrDefault("HOSTNAME", "local");

    // ── 1. Consumer: where records come from ──────────────────────────────────────────
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-enricher");
    consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, pod);          // static membership
    consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

    // ── 2. Producer: where results go (shared by the sink and the DLQ) ────────────────
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);        // keeps per-key order
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

    // ── 3. Dependencies used by the processing step ───────────────────────────────────
    CustomerClient customers = new CustomerClient(URI.create("http://customers:8080"),
        Duration.ofSeconds(2));                                               // own I/O timeout
    PricingDao pricing = new PricingDao(dataSource());                        // JDBC pool

    // ── 4. Output sink and DLQ ────────────────────────────────────────────────────────
    KafkaRecordSink<String, EnrichedOrder> output =                           // new
        KafkaRecordSink.<String, EnrichedOrder>builder()
            .producerProperties(producerProps)
            .topic("orders-enriched")
            .keyFrom(EnrichedOrder::orderId)
            .keySerializer(new StringSerializer())
            .valueSerializer(new EnrichedOrderSerializer())
            .sourceHeader(true)                         // adds kp-source=topic-partition-offset
            .build();

    KafkaDLQHandler<String, Order> dlq =                                         // new
        KafkaDLQHandler.<String, Order>builder()
            .sharing(output)                            // same producer as the output sink
            .topic("orders-dlq")
            .keySerializer(new StringSerializer())
            .valueSerializer(new OrderSerializer())
            .build();                                   // adds error class/message headers

    // ── 5. The pipeline ───────────────────────────────────────────────────────────────
    KafkaPipeline<String, Order> pipeline = new KafkaPipeline<>(
        KafkaPipeline.<String, Order>builder()
            .name("order-enricher")
            .consumerProperties(consumerProps)
            .topic("orders")

            // Concurrency: 500 virtual threads shared by all assigned partitions.
            .concurrency(ThreadMode.VIRTUAL, 500)
            .ordering(ProcessingOrder.KEY)              // one in flight per order id
            .maxConcurrencyPerPartition(100)            // one slow partition can't take all workers

            // ── Record processing: business logic, runs on the workers, may block on I/O ──
            .processor(record -> {                                            // new
              Order order = record.value();
              if (order.isTest()) {
                return List.of();                       // nothing to publish; record is acked
              }
              Customer customer = customers.get(order.customerId());          // blocking HTTP
              Price price = pricing.priceFor(order.sku(), customer.tier());   // blocking JDBC
              return List.of(new EnrichedOrder(order, customer, price));
            })
            .processingTimeout(Duration.ofSeconds(10))  // a hung attempt counts as failed

            // ── Output: the record is acked only after this write is durable ──
            .sink(output)                                                     // new

            // ── Failures: retry, then DLQ; never blocks the partition ──
            .errorStrategy(ErrorStrategy.<String, Order>builder()
                .maxRetries(3)
                .retryBackoff(Duration.ofMillis(200))
                .exponentialBackoff(true)
                .maxBackoff(Duration.ofSeconds(5))
                .dlqHandler(dlq)                                              // KafkaDLQHandler is a DLQHandler
                .onFinalFailure((record, error) ->      // DLQ itself failed: last resort
                    log.error("Dropped {}-{}@{}", record.topic(), record.partition(),
                        record.offset(), error))
                .build())

            // ── Global backpressure: pauses all partitions when work piles up ──
            .recordBackpressure(RecordBackpressureConfig.builder()
                .highWatermark(5_000)
                .lowWatermark(3_000)
                .build())
            .byteBackpressure(ByteBackpressureConfig.builder()
                .highWatermarkBytes(256L << 20)         // 256 MB of in-flight payload
                .lowWatermarkBytes(128L << 20)
                .build())
            // The sink's pending sends are added to the same global controller automatically.

            // ── Offsets and lifecycle ──
            .commitInterval(Duration.ofSeconds(1))
            .drainTimeout(Duration.ofSeconds(20))       // < max.poll.interval.ms
            .shutdownTimeout(Duration.ofSeconds(12))    // ~2x fits a 30 s k8s grace period

            .metricsExporter(metricsExporter())         // e.g. your OTel/Micrometer exporter
            .build());

    // ── 6. Run ────────────────────────────────────────────────────────────────────────
    // Blocks until stop() or SIGTERM. Throws if the brokers are unreachable at startup or a
    // fatal error (auth, fencing, OOM) stops the pipeline. On shutdown it stops the workers,
    // flushes the sink and DLQ, makes the final commit, then closes the sink and the consumer.
    try {
      pipeline.start();
    } catch (RuntimeException fatal) {
      log.error("order-enricher stopped", fatal);
      System.exit(1);                                   // let the orchestrator restart the pod
    } finally {
      // With the class form (OrderEnrichmentProcessor), the pipeline closes these for you.
      customers.close();
      pricing.close();
    }
  }
}
```

What happens to one record: a worker runs `processor` (retried with backoff if it throws). The outputs go to `sink.write`. The worker is then free for the next record while the send is in flight. When the send completes, the record is acked in its partition window, its key slot is released, and the in-flight count drops. If the processor or the write keeps failing, the record goes to `orders-dlq` and is acked, so it never blocks the partition.

The same pipeline with exactly-once output only adds `.exactlyOnce(...)` (§3.2). The processor, the sink and the DLQ do not change; the DLQ sharing the output's producer is what lets a bad record land in the DLQ inside the same transaction as its offset. Downstream consumers of `orders-enriched` then read with `isolation.level=read_committed`.

### 3.2 Exactly-once

```java
    .exactlyOnce(ExactlyOnceConfig.builder()
        .transactionalId("order-enricher-" + pod)           // stable per instance
        .commitInterval(Duration.ofMillis(500))
        .build())
```

Validation when `exactlyOnce` is set:

- The sink must be a `KafkaRecordSink`, and the DLQ (if any) must be a Kafka DLQ on the same producer.
- `skipCompletedOnRestart` must be on. It becomes part of correctness (§5.3).
- `enable.idempotence=true`; `transaction.timeout.ms` must exceed `commitInterval` plus the worst-case barrier wait (§5.4).

### 3.3 Packaging

| Artifact | Contents | Extra dependencies |
|---|---|---|
| `kafka-pipeline` (core) | Sink SPI, `KafkaRecordSink`, Kafka DLQ sink, exactly-once mode | None: `kafka-clients` is already the core dependency |
| `kafka-pipeline-sink-jdbc` | Batched upsert sink | JDBC driver (user's) |
| `kafka-pipeline-sink-s3` | Buffering object sink | AWS SDK v2 |

Kafka lives in core because exactly-once needs the core commit path (§5) and adds no dependency. Anything with third-party dependencies is a separate module behind the SPI.

## 4. At-least-once sink (phase 1)

### 4.1 Record lifecycle

```
poll → register (window: REGISTERED) → dispatch → worker:
   markInProgress → processor.process → sink.write(ctx, outputs) ──► stage
                                                                 │ completes
   ack (window: DONE) ◄── release key/partition slot ◄── counter.completed
```

What changes compared to today:

- **The worker is free as soon as `write` returns its stage.** It does not wait for the broker. Throughput is no longer bounded by threads × write latency.
- **`ack`, `dispatcher.release` and `counter.completed` run on stage completion** instead of in the worker's `finally`. The completion is handed from the sink's I/O thread (for Kafka, the producer's sender thread) to a virtual thread owned by the pipeline, so hooks, acks and DLQ sends never stall the sink.
- **The record stays IN_PROGRESS** in its partition window until completion, so commits stay correct with no new logic.

### 4.2 Backpressure

- **A record counts as in-flight until its write completes** (`InFlightCounter`). A slow sink therefore raises the global count and triggers the existing global throttle. That is the primary mechanism.
- **`sink.sensor()`** reports pressure the pipeline cannot see, such as producer `buffer-available-bytes`, a saturated JDBC pool, or a long S3 upload queue. It is added to the **global** `BackpressureController`. A sink never pauses individual partitions.
- **Per-partition limits stay what they are:** window full, queue full, ordering, and batch limit. A buffering sink's `maxBufferedRecordsPerPartition` is checked against `maxWindowSize` at startup (§4.5).

### 4.3 Ordering

In `KEY` and `PARTITION` mode, a key's or partition's slot is released on write completion. Since the next record of a key cannot start until the previous write is durable, per-key output order is guaranteed for any sink.

For the Kafka sink with `UNORDERED`, the idempotent producer keeps send order per output partition. That is all `UNORDERED` promises.

### 4.4 Failures and idempotency

- **Synchronous failures** (the processor throws, serialization fails) take the existing retry → DLQ → drop path.
- **A failed write stage** is retried by re-running only `write` with the same outputs; the processor is not re-run, so its I/O is not repeated. The backoff is waited out on a pipeline virtual thread, never on the sink's I/O thread. What happens next depends on the failure:
  The sink classifies the failure (`RecordSink.classify` returns a `WriteFailure`; the default `WriteFailure.of` understands Kafka errors, and `KafkaRecordSink` adds "closed producer is fatal"):
  - **`FATAL`** (authentication, authorization such as a missing ACL on the output topic, unsupported version, a closed producer; always for JVM errors): every record would fail the same way, so the pipeline stops and the record stays unacked. Dropping it would commit input whose output was never written.
  - **`TRANSIENT`** (by default Kafka's `RetriableException`, which includes producer timeouts): the destination is down for now. The write is retried (backoff between 100 ms and 30 s) while the pipeline runs and owns the partition, without using the record's retry budget. The record keeps its slot, so an outage becomes backpressure, not drops. If the same write keeps failing transiently for longer than `writeOutageTimeout` (default 5 minutes), the destination is treated as gone and the pipeline stops, still without dropping the record; a misspelled output topic shows up this way instead of stalling forever.
  - **`RECORD`** (anything else, e.g. a record too large): up to `maxRetries`, then the DLQ or a drop with `FinalFailureHandler`. The record is acked either way, so a bad record never blocks the partition.
- **The DLQ is classified the same way** (`DLQHandler.classify`): a transient DLQ failure is retried until the DLQ recovers (or `writeOutageTimeout` stops the pipeline), a fatal one stops the pipeline, and only a record the DLQ rejects is dropped.
- **Abandoned writes.** When the pipeline stops, or the partition is revoked or lost while a write is pending or backing off, the write is abandoned: no further retry, no DLQ, no ack. The record is redelivered to whoever owns the partition next, so an old assignment never rewrites or dead-letters a record the new owner is processing.
- **Idempotency.** Every write carries `SinkContext.recordId()`. First-party sinks use it so that redelivery is harmless:

| Sink | Idempotency mechanism |
|---|---|
| Kafka (at-least-once) | Output key or header `kp-source = topic-partition-offset`; consumers dedup, or use a compacted topic |
| JDBC | `INSERT … ON CONFLICT (source_id) DO UPDATE` |
| S3 | Object key `prefix/topic/partition/{firstOffset}-{lastOffset}`, so a re-upload overwrites the same object |
| HTTP | `Idempotency-Key: recordId` |
| Kafka DLQ | Original key kept; `kp-source` header = `recordId`, so a duplicate DLQ entry can be identified |

### 4.5 Buffering sinks (S3)

A buffering sink completes stages only when it uploads, so records sit unacked for up to its flush interval.

- **Window fit.** If `maxBufferedRecordsPerPartition` exceeds 80% of `maxWindowSize`, the window pause would stop the buffer from ever filling. The builder rejects that config.
- **Flush triggers.** The sink flushes on size or age (its own policy). The pipeline also calls `onPartitionRevoked(tp, budget)` when a partition is revoked, `flush(tp)` after every write of a partition that is being drained, and `flush(tp)` for every assigned partition at shutdown.
- **Revoke.** The pipeline calls `onPartitionRevoked(tp, drainTimeout)`. Whatever is uploaded in time is acked and committed. The rest is dropped and redelivered to the new owner, which is safe because the uploads are idempotent (§4.4). Lost partitions are always dropped.
- **Stall metric.** The sink reports its flush interval, and `PartitionMetrics.commitStallMillis` subtracts it so normal buffering does not look like a stuck record.

### 4.6 DLQ as a sink

`KafkaDLQHandler` ships in core and implements `DLQHandler`, so it plugs into the existing `dlqHandler(...)`; `ErrorStrategy` is unchanged. It waits for the broker before returning, so a record counts as dead-lettered only once it is in the DLQ. If shutdown interrupts a DLQ send, the record is neither dropped nor acked and is redelivered.

### 4.7 Shutdown

1. Stop the workers (no new records processed).
2. `flush` every partition on every sink, bounded by `shutdownTimeout`.
3. Run the final commit. This is correct even if the flush timed out, because only completed writes were acked.
4. Close the sinks, then the consumer.

## 5. Exactly-once (phase 2)

### 5.1 Overview

One transactional `KafkaProducer` per pipeline instance (EOS v2, KIP-447) is shared by all workers. Workers send concurrently into the open transaction. The poll thread periodically commits the outputs **and** the consumed offsets in that transaction, then opens the next one.

```
             T(n) open                         commit tick                 T(n+1) open
workers  ── send/ack ── send/ack ── send/ack ─┤ barrier ├─ send/ack ── send/ack ──
poll thr ─────────────────────────────────────┤ sendOffsetsToTransaction(h, D, groupMeta)
                                               │ commitTransaction · beginTransaction
```

### 5.2 Invariant

For every input record `r`, at every committed transaction boundary:

> **`r`'s outputs are committed ⇔ `r` is marked done in the committed offsets of its partition.**

Here "marked done" means `r.offset < h`, or `r.offset ∈ D`, where `h` is the committed offset and `D` is the set of finished offsets beyond it carried in the commit metadata (`kp1:` bitmap).

If the invariant holds, a restart or a new owner resumes at `h`, skips exactly `D`, and reprocesses only records whose outputs were never committed. Each output is therefore committed exactly once. The rest of this section is what maintains it.

### 5.3 Out-of-order completion: the commit metadata carries `D`

Workers finish records out of order, so at commit time there are finished records beyond the window head. Their outputs are in the transaction. If only `h` were committed, a crash would reprocess them and duplicate their output.

The existing completed-offsets bitmap solves this. `sendOffsetsToTransaction` takes `OffsetAndMetadata` with metadata, so `(h, D)` is committed atomically with the outputs. On assign, the existing replay path reads `D` and registers those offsets as DONE without dispatching them.

That is why `skipCompletedOnRestart` cannot be turned off in this mode, and why the at-least-once fallbacks become errors:

| Situation | At-least-once today | Exactly-once |
|---|---|---|
| Bitmap too large to encode | Commit `h` only (reprocessing is fine) | Cannot happen, because of the span limit below; if it does, fatal |
| Broker rejects metadata (`OffsetMetadataTooLarge`) | Stop tracking, log a warning | Fatal: stop the pipeline |
| Metadata cannot be read on assign | Reprocess | Fatal for that partition: keep it paused and retry |

**Span limit.** The bitmap is offset-relative: it covers `highestDone − h`, not the window's entry count. On compacted topics those differ, because offsets have gaps. The encoded form is capped at 4,000 characters, which is about 2,970 bytes after Base64 and enough for about 23,000 offsets even when the bitmap does not compress. In exactly-once mode, each partition window gets an extra per-partition pause when `highestRegistered − h` reaches 20,000, and resumes at 10,000. This is a per-partition limit like the existing window pause, not a substitute for global backpressure. With it, the metadata always fits. `offset.metadata.max.bytes` (broker default 4096) must not be lowered; if it is, the first commit fails fatally.

### 5.4 The commit barrier

A record with two outputs must never have one output in `T(n)` and the other in `T(n+1)`, or straddle a commit. The same applies to an output and the record's ack.

- **Workers** enter a short critical section under the **shared** side of a fair `ReentrantReadWriteLock`:
  1. Check that the record's generation is still current for its partition; if not, discard (§5.6).
  2. `send` all outputs. These are pre-serialized bytes (§5.5), so `send` only enqueues.
  3. `ack` the record in its window.

  Record processing (the processor, including its I/O) runs **outside** the lock; only the enqueue of already-computed outputs and the ack are inside.
- **The committer** (poll thread, at each commit tick) takes the **exclusive** side, then:
  1. Reads `(h, D)` for every assigned partition. Because no worker is mid-section, every record marked done has its outputs in `T(n)` or earlier, and every output in `T(n)` belongs to a record marked done.
  2. `sendOffsetsToTransaction(points, consumer.groupMetadata())`, `commitTransaction()`, `beginTransaction()`.
  3. Releases the lock.

**Cost.** Sends pause while the commit round-trip runs, typically a few to tens of milliseconds per `commitInterval`. At 500 ms intervals that is a 1–5% duty cycle. Record processing continues throughout; only the hand-off of finished outputs waits. Use the fair lock so that a steady stream of workers cannot starve the committer. `transaction.timeout.ms` must exceed `commitInterval` plus the longest critical section, which is bounded by `max.block.ms` when the producer buffer is full.

**Why acks can happen right after send.** In this mode a worker acks right after `send()` returns, without waiting for the broker. If any send fails, `commitTransaction` fails and the whole transaction aborts (§5.6), so an ack can never outlive a lost output.

### 5.5 Failures attributable to one record

A failure tied to one record must not abort everyone's transaction, so everything that can fail per record is moved to **before** the critical section:

- **Serialization.** The sink serializes key and value to `byte[]` outside the lock (the producer uses `ByteArraySerializer`). A `SerializationException` goes through the normal retry → DLQ → drop path, and no output has been sent yet.
- **Size.** The serialized size is checked against `max.request.size` before sending, so a would-be `RecordTooLargeException` is also a per-record failure.
- **DLQ inside the transaction.** A DLQ write is an output of the failed record, sent in the same critical section as its ack. A bad record therefore lands in the DLQ exactly once and its offset is committed with it, so it never blocks the partition.

What remains are broker-side, transaction-wide errors.

### 5.6 Abort and reset

When a send callback fails, `commitTransaction` throws, or partitions are lost, the committer does the following under the exclusive lock:

1. `abortTransaction()`.
2. For every assigned partition: drop its queue, clear its window (bumping the generation), seek to the last committed `h`, and re-initialize the window with the committed `D` as pre-done. This is the existing revoke/assign path, run locally.
3. `beginTransaction()` and release the lock.

Records still in flight from the old generation fail the generation check at step 1 of their critical section and are discarded, so a late worker cannot send into `T(n+1)`. The existing generation-checked acks provide this.

Fatal errors stop the pipeline through the `FatalErrors` path: `ProducerFencedException`, `InvalidProducerEpochException`, authorization errors, and **N consecutive aborts with no successful commit** (default 5). The last one prevents an endless abort loop caused by a broker-side error that will never clear.

### 5.7 Rebalances

| Callback | Action |
|---|---|
| `onPartitionsRevoked` | Drain revoked partitions (existing, `drainTimeout`), then take the barrier and commit `T(n)` for all assigned partitions; clear the revoked ones. Stragglers fail the generation check. |
| `onPartitionsLost` | Fencing is likely, so **abort and reset** (§5.6). Never try to commit. |
| `onPartitionsAssigned` | Existing per-partition init: read `(h, D)` with the bounded budget and mark `D` as pre-done. |

`consumer.groupMetadata()` is read on the poll thread at commit time, so a zombie whose generation is stale is fenced by the coordinator.

### 5.8 What stays the same

Global backpressure, per-key ordering (the key slot is released on ack, inside the critical section), `processingTimeout`, metrics, and fail-fast start all keep working unchanged. The input consumer may use `read_committed` if upstream is transactional. This is orthogonal.

## 6. Internal changes

- **`CommitStrategy`** (internal role interface, poll thread only), so `PeriodicCommitter` and the rebalance listener depend on a role, not on a class:

  ```java
  interface CommitStrategy extends AutoCloseable {
    void start();
    void maybeCommit();                                  // commit tick
    void onRevoked(Collection<TopicPartition> revoked);  // after drain
    void onLost(Collection<TopicPartition> lost);
    void commitFinal();
  }
  ```

  Implementations: `ConsumerCommitStrategy` (today's `PeriodicCommitter`) and `TransactionalCommitStrategy`.
- **`CompletionHandler`.** The worker pools stop calling ack, release and counter updates in `finally` and delegate to a single place that runs on write completion. The handler-only path completes immediately, so its behavior is unchanged.
- **`SinkWriteGate`.** The shared/exclusive barrier. Its at-least-once implementation is a no-op.
- **Per-partition span pause** in `PartitionPauseController`, as one more hold, active only in exactly-once mode.
- **`CompletedOffsetsCodec`** gets a strict mode that throws instead of returning `""`.

## 7. Metrics (via `MetricsExporter`)

- `sink.write.latency` (timer), `sink.pending.writes`, `sink.write.failures`
- exactly-once: `txn.commit.latency`, `txn.commits`, `txn.aborts`, `txn.barrier.wait`
- buffering sinks: `sink.buffered.records` per partition

## 8. Testing

- **Unit tests** with `MockProducer` in transactional mode:
  - the invariant under random completion order;
  - a crash between send and commit;
  - send failure → abort → reset, and stragglers are discarded;
  - serialization and size errors stay per record;
  - the span pause;
  - strict codec mode.
- **Concurrency stress test.** Thousands of records with random latency, a random abort injected every K commits; check that the read_committed output equals the input exactly once (keyed by `recordId`).
- **Broker tests** (Testcontainers):
  - `kill -9` mid-stream, then restart;
  - a member joining and leaving;
  - a zombie with a stale generation gets fenced;
  - an admin removes a static member;
  - a poison record goes to the DLQ exactly once.

  Each verified by reading the output with `read_committed` and asserting each input produced exactly one output.

## 9. Rollout

| Phase | Scope | Release |
|---|---|---|
| 1 | Sink SPI, `RecordProcessor`, async completion, `KafkaRecordSink` (at-least-once), Kafka DLQ sink, `CommitStrategy` refactor (no behavior change) | 0.3.0 |
| 2 | `exactlyOnce()`: transactional strategy, barrier, strict metadata, span pause, abort/reset | 0.4.0 |
| 3 | `kafka-pipeline-sink-jdbc`, `kafka-pipeline-sink-s3` | 0.4.x |

Phase 1 is additive: existing `handler(...)` users see no change.

## 10. Alternatives considered

- **One transactional producer per partition (EOS v1).** Simpler fencing, but N producers per instance. KIP-447 makes one per instance sufficient.
- **One transaction per record.** No barrier needed, but a commit round-trip for every record. Far too slow.
- **Waiting for send callbacks before acking in exactly-once mode.** Not needed; the transaction already gives atomicity (§5.4), and waiting would add latency.
- **Streams-style serial processing per task.** Correct, but it gives up the library's reason to exist.

## 11. Open questions

1. Default `transactionalId`: require it, or derive it from `group.instance.id` when static membership is on?
2. Should exactly-once allow a non-Kafka DLQ (best-effort, at-least-once for DLQ only), or require a Kafka DLQ?
3. Batch mode: one critical section per batch is natural. Does the batch processor need its own API, or is a list of outputs per input enough?
4. Span limit of 20,000: fixed, or derived from the encoded size of the bitmap at runtime?
