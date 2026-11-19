plugins {
    id("kafka-pipeline.java-library")
}

dependencies {
    api("org.apache.kafka:kafka-clients:4.4.0")
}

mavenPublishing {
    // Unchanged from the single-module releases, so existing users keep their dependency.
    coordinates(artifactId = "kafka-pipeline")
    pom {
        name.set("kafka-pipeline")
        description.set(
            "Concurrent Kafka consumer pipeline: per-key ordering, global backpressure, sliding-window" +
                " offsets, error handling, and a sink SPI with a Kafka sink")
    }
}

