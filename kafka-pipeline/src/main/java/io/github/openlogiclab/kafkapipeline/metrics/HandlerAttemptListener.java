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
 * Receives each handler attempt. See {@link MetricsBinding#onHandlerAttempt}.
 *
 * <p>Runs on the worker thread: keep it fast and non-blocking. Exceptions it throws are logged and
 * ignored, never passed to the pipeline.
 */
@FunctionalInterface
public interface HandlerAttemptListener {

  /**
   * Called after one handler attempt.
   *
   * @param attempt the attempt that just finished
   */
  void onAttempt(HandlerAttempt attempt);
}
