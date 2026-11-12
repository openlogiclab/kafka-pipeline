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

import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import org.apache.kafka.common.TopicPartition;

/**
 * Observes each handler attempt. {@code RetryExecutor} reports to this, and does not know who
 * listens: the metrics collector, exporters, or both, combined with {@link #andThen}.
 *
 * <p>Takes plain values rather than an event object so that observers that need no object, such as
 * the collector, cost no allocation.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
@FunctionalInterface
public interface HandlerAttemptObserver {

  /** Observes nothing. */
  HandlerAttemptObserver NONE = (tp, nanos, outcome) -> {};

  /**
   * Called after one handler attempt.
   *
   * @param tp the partition the records came from
   * @param nanos how long the attempt ran, in nanoseconds
   * @param outcome how the attempt ended
   */
  void onAttempt(TopicPartition tp, long nanos, AttemptOutcome outcome);

  /**
   * Returns an observer that calls this one, then {@code next}.
   *
   * @param next the observer to call second
   * @return the combined observer
   */
  default HandlerAttemptObserver andThen(HandlerAttemptObserver next) {
    return (tp, nanos, outcome) -> {
      onAttempt(tp, nanos, outcome);
      next.onAttempt(tp, nanos, outcome);
    };
  }
}
