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

import io.github.openlogiclab.kafkapipeline.internal.ExporterBinding;

/**
 * What a pipeline offers a {@link MetricsExporter}: a name, a snapshot to pull, and events to
 * subscribe to.
 *
 * <p>This interface is sealed: only the pipeline creates bindings, so new events can be added in
 * later releases without breaking exporters.
 */
public sealed interface MetricsBinding permits ExporterBinding {

  /**
   * The pipeline's name, for tagging its metrics.
   *
   * @return the pipeline name
   */
  String pipelineName();

  /**
   * Current metrics. Cheap and thread-safe; call it on every collection.
   *
   * @return a new snapshot
   */
  PipelineMetrics snapshot();

  /**
   * Subscribes to handler attempts. The listener runs on the worker thread, right after each
   * attempt, so keep it fast and non-blocking: record into a histogram, nothing more.
   *
   * @param listener receives every handler attempt from now on
   */
  void onHandlerAttempt(HandlerAttemptListener listener);
}
