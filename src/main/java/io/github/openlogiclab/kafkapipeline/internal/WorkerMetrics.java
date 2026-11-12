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

/**
 * Record outcomes the worker pools record. Failure outcomes are recorded by {@link RetryMetrics}.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public interface WorkerMetrics {

  /**
   * Records records whose handler succeeded.
   *
   * @param count number of records processed
   */
  void recordProcessed(long count);

  /**
   * Records records skipped by the lifecycle hook. Not for records dropped after a failure; those
   * go to {@link RetryMetrics#recordFailed}.
   *
   * @param count number of records skipped
   */
  void recordSkipped(int count);
}
