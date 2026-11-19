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

import java.time.Duration;
import java.util.Objects;

/** Validation of the durations users configure. */
public final class Durations {

  /**
   * Longest accepted duration: 100 years. Longer values mean "never" in practice, and durations are
   * added to {@code System.nanoTime()} to make deadlines, which must not overflow; a {@code
   * Duration} itself goes up to billions of years ({@code ChronoUnit.FOREVER}).
   */
  public static final Duration MAX = Duration.ofDays(36_500);

  private Durations() {}

  /**
   * Checks that a duration is positive and at most {@link #MAX}.
   *
   * @param d the duration
   * @param name its setting, for the error message
   * @return the duration
   * @throws IllegalArgumentException if it is not
   */
  public static Duration requirePositive(Duration d, String name) {
    requireNonNegative(d, name);
    if (d.isZero()) throw new IllegalArgumentException(name + " must be positive, got " + d);
    return d;
  }

  /**
   * Checks that a duration is zero or positive and at most {@link #MAX}.
   *
   * @param d the duration
   * @param name its setting, for the error message
   * @return the duration
   * @throws IllegalArgumentException if it is not
   */
  public static Duration requireNonNegative(Duration d, String name) {
    Objects.requireNonNull(d, name);
    if (d.isNegative()) {
      throw new IllegalArgumentException(name + " must not be negative, got " + d);
    }
    if (d.compareTo(MAX) > 0) {
      throw new IllegalArgumentException(name + " must be at most 100 years, got " + d);
    }
    return d;
  }
}
