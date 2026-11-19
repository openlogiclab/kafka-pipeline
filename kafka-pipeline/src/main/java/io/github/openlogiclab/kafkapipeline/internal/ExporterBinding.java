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
package io.github.openlogiclab.kafkapipeline.internal;

import io.github.openlogiclab.kafkapipeline.metrics.HandlerAttemptListener;
import io.github.openlogiclab.kafkapipeline.metrics.MetricsBinding;
import io.github.openlogiclab.kafkapipeline.metrics.PipelineMetrics;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The only {@link MetricsBinding} implementation: the pipeline's name, its snapshot source, and the
 * publisher that exporters subscribe to.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class ExporterBinding implements MetricsBinding {

  private final String pipelineName;
  private final Supplier<PipelineMetrics> snapshots;
  private final AttemptPublisher attempts;

  /**
   * Creates a binding.
   *
   * @param pipelineName the pipeline name
   * @param snapshots source of metrics snapshots
   * @param attempts publisher that handler-attempt listeners subscribe to
   */
  public ExporterBinding(
      String pipelineName, Supplier<PipelineMetrics> snapshots, AttemptPublisher attempts) {
    this.pipelineName = Objects.requireNonNull(pipelineName, "pipelineName");
    this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
    this.attempts = Objects.requireNonNull(attempts, "attempts");
  }

  @Override
  public String pipelineName() {
    return pipelineName;
  }

  @Override
  public PipelineMetrics snapshot() {
    return snapshots.get();
  }

  @Override
  public void onHandlerAttempt(HandlerAttemptListener listener) {
    attempts.subscribe(listener);
  }
}
