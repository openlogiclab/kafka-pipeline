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
package io.github.openlogiclab.kafkapipeline.worker;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Interrupts a handler attempt that runs longer than the processing timeout.
 *
 * <p>The interrupt is delivered under the attempt's monitor and only while the attempt is still
 * running, so a timer that fires just as the attempt finishes never leaves a stray interrupt on the
 * worker thread: {@link Attempt#finish()} clears any interrupt the timer delivered.
 *
 * <p>A handler that ignores interrupts cannot be stopped; it keeps its worker until it returns.
 */
final class AttemptTimer {

  /** A timer that never fires; used when no processing timeout is configured. */
  static final AttemptTimer DISABLED = new AttemptTimer(Duration.ZERO);

  private final long timeoutNanos;
  private final ScheduledThreadPoolExecutor scheduler;

  AttemptTimer(Duration timeout) {
    this.timeoutNanos = timeout == null ? 0 : timeout.toNanos();
    if (timeoutNanos > 0) {
      scheduler =
          new ScheduledThreadPoolExecutor(
              1,
              r -> {
                Thread t = new Thread(r, "kafka-pipeline-timeout");
                t.setDaemon(true);
                return t;
              });
      // Most attempts finish well before their deadline; drop cancelled timers right away.
      scheduler.setRemoveOnCancelPolicy(true);
    } else {
      scheduler = null;
    }
  }

  boolean enabled() {
    return scheduler != null;
  }

  Duration timeout() {
    return Duration.ofNanos(timeoutNanos);
  }

  /** Starts timing an attempt on the current thread. Call {@link Attempt#finish()} when done. */
  Attempt start() {
    Attempt attempt = new Attempt(Thread.currentThread());
    if (scheduler != null) {
      try {
        attempt.timer = scheduler.schedule(attempt::fire, timeoutNanos, TimeUnit.NANOSECONDS);
      } catch (RejectedExecutionException e) {
        // Closed during shutdown while a worker still runs: time nothing rather than fail.
      }
    }
    return attempt;
  }

  void close() {
    if (scheduler != null) scheduler.shutdownNow();
  }

  /** One timed attempt. */
  static final class Attempt {
    private final Thread thread;
    private ScheduledFuture<?> timer; // set right after construction, before the timer can fire
    private boolean finished; // guarded by this
    private boolean timedOut; // guarded by this

    private Attempt(Thread thread) {
      this.thread = thread;
    }

    private synchronized void fire() {
      if (finished) return;
      timedOut = true;
      thread.interrupt();
    }

    /**
     * Stops the timer. If it already fired, clears the interrupt it delivered.
     *
     * @return true if the attempt ran past its deadline
     */
    synchronized boolean finish() {
      finished = true;
      if (timer != null) timer.cancel(false);
      if (timedOut) Thread.interrupted();
      return timedOut;
    }
  }
}
