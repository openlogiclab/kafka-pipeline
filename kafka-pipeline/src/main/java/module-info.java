/**
 * kafka-pipeline — a lightweight Kafka consumer wrapper that provides concurrent processing,
 * backpressure, and offset management out of the box.
 *
 * <p>Exported packages are the public API: the root package, {@code metrics}, {@code handler},
 * {@code error}, {@code backpressure}, {@code sink} (the sink SPI) and {@code sink.kafka} (the
 * Kafka sink, an implementation of that SPI). Everything else ({@code consumer}, {@code commit},
 * {@code dispatch}, {@code worker}, {@code offset}, {@code internal}, {@code
 * internal.backpressure}) is internal and can change in any release. Each package's {@code
 * package-info.java} describes its role and the threads it runs on.
 */
// kafka-clients is an automatic module (no module-info of its own); requiring it transitively is
// still right, because exported types use Kafka types.
@SuppressWarnings("requires-transitive-automatic")
module io.github.openlogiclab.kafkapipeline {
  // Transitive: exported types use Kafka types (ConsumerRecord, TopicPartition, Producer, ...).
  requires transitive kafka.clients;
  requires java.management;
  // Optional: heap usage right after the last GC (HeapSensor). Without it the sensor falls back to
  // current heap usage.
  requires static jdk.management;

  exports io.github.openlogiclab.kafkapipeline;
  exports io.github.openlogiclab.kafkapipeline.metrics;
  exports io.github.openlogiclab.kafkapipeline.handler;
  exports io.github.openlogiclab.kafkapipeline.error;
  exports io.github.openlogiclab.kafkapipeline.backpressure;
  exports io.github.openlogiclab.kafkapipeline.sink;
  exports io.github.openlogiclab.kafkapipeline.sink.kafka;
}
