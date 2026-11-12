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

import io.github.openlogiclab.kafkapipeline.metrics.MetricsBinding;
import io.github.openlogiclab.kafkapipeline.metrics.MetricsExporter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Binds a pipeline's exporters when it starts and closes them when it has shut down.
 *
 * <p>A failing {@code bind} propagates, so a misconfigured exporter fails {@code start()}; the
 * exporters bound before it are still closed by {@link #closeAll()}. A failing {@code close} is
 * logged and the remaining exporters are still closed. Exporters are closed in reverse bind order.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class MetricsExporters {

  private static final System.Logger logger = System.getLogger(MetricsExporters.class.getName());

  private final List<MetricsExporter> exporters;
  private final Deque<MetricsExporter> bound = new ArrayDeque<>();

  /**
   * Creates the lifecycle for the given exporters.
   *
   * @param exporters the exporters, in registration order
   */
  public MetricsExporters(List<MetricsExporter> exporters) {
    this.exporters = List.copyOf(exporters);
  }

  /**
   * Binds every exporter, in registration order.
   *
   * @param binding the pipeline's metrics
   */
  public synchronized void bindAll(MetricsBinding binding) {
    for (MetricsExporter exporter : exporters) {
      exporter.bind(binding);
      bound.push(exporter);
    }
  }

  /** Closes every bound exporter, in reverse bind order. Safe to call more than once. */
  public synchronized void closeAll() {
    while (!bound.isEmpty()) {
      MetricsExporter exporter = bound.pop();
      try {
        exporter.close();
      } catch (Throwable t) {
        FatalErrors.rethrowIfJvmFatal(t);
        logger.log(System.Logger.Level.WARNING, "Error closing metrics exporter " + exporter, t);
      }
    }
  }
}
