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
import io.github.openlogiclab.kafkapipeline.metrics.HandlerAttemptListener;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.TopicPartition;

/**
 * Passes handler attempts to the listeners exporters subscribed, keeping each listener's failures
 * away from the worker thread.
 *
 * <p>With no listeners it does nothing and allocates nothing. A listener that throws is logged once
 * at WARNING, then at DEBUG, so a broken exporter cannot flood the log. A JVM-fatal error such as
 * {@link OutOfMemoryError} is rethrown, as everywhere else in the pipeline.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public final class AttemptPublisher implements HandlerAttemptObserver {

  private static final System.Logger logger = System.getLogger(AttemptPublisher.class.getName());

  private final List<GuardedListener> listeners = new CopyOnWriteArrayList<>();

  /** Creates a publisher with no listeners. */
  public AttemptPublisher() {}

  /**
   * Adds a listener for every attempt from now on.
   *
   * @param listener the listener
   */
  void subscribe(HandlerAttemptListener listener) {
    listeners.add(new GuardedListener(Objects.requireNonNull(listener, "listener")));
  }

  @Override
  public void onAttempt(TopicPartition tp, long nanos, AttemptOutcome outcome) {
    if (listeners.isEmpty()) return;
    HandlerAttemptEvent event = new HandlerAttemptEvent(tp, nanos, outcome);
    for (GuardedListener listener : listeners) {
      listener.accept(event);
    }
  }

  private static final class GuardedListener {
    private final HandlerAttemptListener delegate;
    private final AtomicBoolean warned = new AtomicBoolean();

    GuardedListener(HandlerAttemptListener delegate) {
      this.delegate = delegate;
    }

    void accept(HandlerAttemptEvent event) {
      try {
        delegate.onAttempt(event);
      } catch (Throwable t) {
        FatalErrors.rethrowIfJvmFatal(t);
        System.Logger.Level level =
            warned.compareAndSet(false, true)
                ? System.Logger.Level.WARNING
                : System.Logger.Level.DEBUG;
        logger.log(level, "Metrics listener " + delegate + " failed; ignoring", t);
      }
    }
  }
}
