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
package io.github.openlogiclab.kafkapipeline;

import java.util.concurrent.atomic.LongAdder;

/**
 * Single source of truth for in-flight record count and byte count.
 *
 * <p>Lock-free, thread-safe. Writers call {@link #registered} / {@link #completed}. Backpressure
 * sensors read {@link #records()} / {@link #bytes()} — they never write.
 *
 * <p>This class knows nothing about backpressure, thresholds, or flow control. It is a pure
 * counter.
 */
public final class InFlightCounter {

  // LongAdder uses cell-striping internally: each thread writes to its own cell,
  // avoiding cache-line contention that AtomicLong/AtomicInteger suffer under
  // high concurrency (200+ worker threads all calling completed() concurrently).
  // sum() is not a point-in-time snapshot, but backpressure sensors only need
  // approximate counts — the hysteresis band absorbs the small read skew.
  private final LongAdder records = new LongAdder();
  private final LongAdder bytes = new LongAdder();

  public void registered(int recordCount, long byteCount) {
    records.add(recordCount);
    bytes.add(byteCount);
  }

  public void completed(int recordCount, long byteCount) {
    records.add(-recordCount);
    bytes.add(-byteCount);
  }

  /**
   * Approximate in-flight count — precision is not required; used only for backpressure decisions.
   * {@code sum()} iterates all internal cells and adds them up; concurrent writes may land between
   * cell reads, so the result can be off by a few records.
   */
  public int records() {
    return (int) records.sum();
  }

  /**
   * Approximate in-flight bytes — precision is not required; used only for backpressure decisions.
   * Same cell-summation semantics as {@link #records()}.
   */
  public long bytes() {
    return bytes.sum();
  }
}
