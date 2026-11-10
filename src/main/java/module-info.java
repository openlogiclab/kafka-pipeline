/**
 * kafka-pipeline — a lightweight Kafka consumer wrapper that provides concurrent processing,
 * backpressure, and offset management out of the box.
 *
 * <p>Exported packages contain the public API for library consumers. Internal packages ({@code
 * worker}, {@code dispatch}, {@code offset}, {@code internal}) are encapsulated and not accessible
 * to downstream modules.
 */
module io.github.openlogiclab.kafkapipeline {
  requires kafka.clients;
  requires java.management;
  // Optional: heap usage right after the last GC (HeapSensor). Without it the sensor falls back to
  // current heap usage.
  requires static jdk.management;

  exports io.github.openlogiclab.kafkapipeline;
  exports io.github.openlogiclab.kafkapipeline.handler;
  exports io.github.openlogiclab.kafkapipeline.error;
  exports io.github.openlogiclab.kafkapipeline.backpressure;
}
