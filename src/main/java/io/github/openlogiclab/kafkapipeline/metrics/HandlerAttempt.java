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

import io.github.openlogiclab.kafkapipeline.internal.HandlerAttemptEvent;
import java.time.Duration;
import org.apache.kafka.common.TopicPartition;

/**
 * One handler attempt: a single-record handler call or a batch handler call. Each retry is a
 * separate attempt.
 *
 * <p>This interface is sealed: only the pipeline creates attempts, so details can be added in later
 * releases without breaking listeners.
 */
public sealed interface HandlerAttempt permits HandlerAttemptEvent {

  /**
   * The partition the records came from.
   *
   * @return the partition
   */
  TopicPartition partition();

  /**
   * How long the handler ran, in nanoseconds.
   *
   * @return the duration in nanoseconds
   */
  long durationNanos();

  /**
   * How long the handler ran.
   *
   * @return the duration
   */
  default Duration duration() {
    return Duration.ofNanos(durationNanos());
  }

  /**
   * How the attempt ended.
   *
   * @return the outcome
   */
  AttemptOutcome outcome();
}
