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

import static org.junit.jupiter.api.Assertions.*;

import io.github.openlogiclab.kafkapipeline.metrics.AttemptOutcome;
import io.github.openlogiclab.kafkapipeline.metrics.HandlerAttempt;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class AttemptPublisherTest {

  private static final TopicPartition TP0 = new TopicPartition("t", 0);

  private final AttemptPublisher publisher = new AttemptPublisher();

  @Test
  void publishesToEveryListenerInOrder() {
    List<String> seen = new ArrayList<>();
    publisher.subscribe(a -> seen.add("a:" + a.outcome()));
    publisher.subscribe(a -> seen.add("b:" + a.outcome()));

    publisher.onAttempt(TP0, 42, AttemptOutcome.TIMEOUT);

    assertEquals(List.of("a:TIMEOUT", "b:TIMEOUT"), seen);
  }

  @Test
  void eventCarriesTheAttempt() {
    List<HandlerAttempt> seen = new ArrayList<>();
    publisher.subscribe(seen::add);

    publisher.onAttempt(TP0, 1_500_000, AttemptOutcome.SUCCESS);

    HandlerAttempt a = seen.get(0);
    assertEquals(TP0, a.partition());
    assertEquals(1_500_000, a.durationNanos());
    assertEquals(java.time.Duration.ofNanos(1_500_000), a.duration());
  }

  @Test
  void throwingListenerIsIsolated() {
    List<HandlerAttempt> seen = new ArrayList<>();
    publisher.subscribe(
        a -> {
          throw new RuntimeException("boom");
        });
    publisher.subscribe(seen::add);

    assertDoesNotThrow(() -> publisher.onAttempt(TP0, 1, AttemptOutcome.FAILURE));
    assertDoesNotThrow(() -> publisher.onAttempt(TP0, 1, AttemptOutcome.FAILURE));
    assertEquals(2, seen.size());
  }

  @Test
  void jvmFatalErrorIsRethrown() {
    publisher.subscribe(
        a -> {
          throw new OutOfMemoryError("fatal");
        });

    assertThrows(OutOfMemoryError.class, () -> publisher.onAttempt(TP0, 1, AttemptOutcome.SUCCESS));
  }

  @Test
  void observersCombineWithAndThen() {
    List<String> order = new ArrayList<>();
    HandlerAttemptObserver first = (tp, n, o) -> order.add("first");
    HandlerAttemptObserver second = (tp, n, o) -> order.add("second");

    first.andThen(second).onAttempt(TP0, 1, AttemptOutcome.SUCCESS);

    assertEquals(List.of("first", "second"), order);
  }
}
