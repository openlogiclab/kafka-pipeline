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
package io.github.openlogiclab.kafkapipeline.metrics;

/**
 * Publishes a pipeline's metrics to a monitoring system: Micrometer, OpenTelemetry, a log, or an
 * in-house system.
 *
 * <p>Register it with {@link io.github.openlogiclab.kafkapipeline.PipelineConfig.Builder
 * #metricsExporter(MetricsExporter)}. The pipeline calls {@link #bind} once when it starts and
 * {@link #close} once when it has shut down, after the final commit. In between, the exporter reads
 * what it needs from the {@link MetricsBinding}:
 *
 * <ul>
 *   <li><b>Pull</b>: read {@link MetricsBinding#snapshot()} whenever the monitoring system
 *       collects, for counters and gauges. Reading is cheap and thread-safe.
 *   <li><b>Push</b>: subscribe with {@link MetricsBinding#onHandlerAttempt} to receive each handler
 *       attempt, for histograms and percentiles.
 * </ul>
 *
 * <p>An exporter that throws from {@code bind} fails {@code start()}, since that is a setup error.
 * After that, a failing exporter never affects processing: exceptions from listeners and from
 * {@code close} are logged and ignored.
 *
 * <p>An exporter registered with several pipelines is bound once per pipeline; tell them apart with
 * {@link MetricsBinding#pipelineName()}.
 */
public interface MetricsExporter extends AutoCloseable {

  /**
   * Connects this exporter to a starting pipeline.
   *
   * @param binding the pipeline's metrics
   */
  void bind(MetricsBinding binding);

  /** Releases what {@link #bind} registered, for example meters or a reporting thread. */
  @Override
  default void close() {}
}
