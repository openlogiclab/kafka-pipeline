rootProject.name = "kafka-pipeline-parent"

// Shared build conventions (kafka-pipeline.java-library), applied by every module.
includeBuild("build-logic")

// Everything Kafka: the pipeline, the sink SPI, the Kafka sink and the Kafka DLQ.
include("kafka-pipeline")

// Sinks for other destinations go in their own modules (for example kafka-pipeline-jdbc), so their
// dependencies stay out of kafka-pipeline.
