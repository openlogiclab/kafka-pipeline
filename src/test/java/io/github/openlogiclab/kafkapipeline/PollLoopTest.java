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

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureConfig;
import io.github.openlogiclab.kafkapipeline.backpressure.BackpressureController;
import io.github.openlogiclab.kafkapipeline.dispatch.PartitionPauseController;
import io.github.openlogiclab.kafkapipeline.dispatch.PeriodicCommitter;
import io.github.openlogiclab.kafkapipeline.internal.NoOpMetricsCollector;
import io.github.openlogiclab.kafkapipeline.offset.UnorderedOffsetTracker;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PollLoopTest {

  private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  private PollLoop<String, String> loop() {
    UnorderedOffsetTracker tracker = new UnorderedOffsetTracker();
    return new PollLoop<>(
        consumer,
        Duration.ofMillis(1),
        new PeriodicCommitter(
            tracker, consumer, Duration.ofSeconds(60), NoOpMetricsCollector.INSTANCE),
        new BackpressureController(BackpressureConfig.defaults(), List.of()),
        new PartitionPauseController(consumer, tracker),
        null, // never reached: every poll below fails
        NoOpMetricsCollector.INSTANCE);
  }

  /** True for the first {@code n} checks, then false. */
  private static BooleanSupplier runningFor(int n) {
    AtomicInteger checks = new AtomicInteger();
    return () -> checks.incrementAndGet() <= n;
  }

  @Test
  void pollErrorWhileStopping_exitsWithoutBackingOff() {
    PollLoop<String, String> loop = loop();
    consumer.setPollException(new KafkaException("broker gone"));

    long start = System.nanoTime();
    loop.run(runningFor(1)); // true for the loop check, false for the error check

    assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 100, "no back-off");
  }

  @Test
  void interruptedDuringBackOff_exitsLoopAndKeepsInterruptFlag() {
    PollLoop<String, String> loop = loop();
    consumer.setPollException(new KafkaException("broker gone"));
    Thread.currentThread().interrupt();

    loop.run(() -> true);

    assertTrue(Thread.currentThread().isInterrupted());
  }

  @Test
  void wakeupWhileStopping_exitsQuietly() {
    PollLoop<String, String> loop = loop();
    consumer.wakeup();

    assertDoesNotThrow(() -> loop.run(runningFor(1)));
  }
}
