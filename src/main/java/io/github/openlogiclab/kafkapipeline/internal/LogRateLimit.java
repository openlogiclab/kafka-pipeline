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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lets a warning through at most once per interval, so a condition that repeats on every poll (a
 * full queue, a partition that cannot be registered) logs one line, not thousands.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class LogRateLimit {

  /** Interval between repeated warnings of one kind: {@value} seconds. */
  public static final long DEFAULT_INTERVAL_SECONDS = 10;

  private final long intervalNanos;
  private final AtomicLong last;

  /** Creates a limiter with the default interval; the first call is let through. */
  public LogRateLimit() {
    this.intervalNanos = TimeUnit.SECONDS.toNanos(DEFAULT_INTERVAL_SECONDS);
    this.last = new AtomicLong(System.nanoTime() - intervalNanos);
  }

  /**
   * Returns true if a warning may be logged now, and if so starts a new interval. Thread-safe: of
   * several threads calling at once, at most one gets true.
   *
   * @return whether to log
   */
  public boolean tryAcquire() {
    long now = System.nanoTime();
    long previous = last.get();
    return now - previous >= intervalNanos && last.compareAndSet(previous, now);
  }
}
