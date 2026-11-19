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
package io.github.openlogiclab.kafkapipeline.error;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CompletionException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;

class WriteFailureTest {

  @Test
  void kafkaErrors_areClassifiedThroughWrappers() {
    assertEquals(
        WriteFailure.TRANSIENT,
        WriteFailure.of(new CompletionException(new TimeoutException("expired"))));
    assertEquals(
        WriteFailure.FATAL,
        WriteFailure.of(new CompletionException(new TopicAuthorizationException("out"))));
    assertEquals(WriteFailure.RECORD, WriteFailure.of(new RecordTooLargeException()));
  }

  @Test
  void unusableProducer_isFatalEvenWhenWrapped() {
    ProducerUnusableException closed =
        new ProducerUnusableException(new IllegalStateException("producer closed"));
    assertEquals(WriteFailure.FATAL, WriteFailure.of(closed));
    assertEquals(WriteFailure.FATAL, WriteFailure.of(new CompletionException(closed)));
  }

  @Test
  void causeCycle_isClassifiedWithoutLoopingForever() {
    assertTimeoutPreemptively(
        java.time.Duration.ofSeconds(2),
        () -> {
          RuntimeException a = new RuntimeException("a");
          RuntimeException b = new RuntimeException("b", a);
          a.initCause(b); // a -> b -> a
          assertEquals(WriteFailure.RECORD, WriteFailure.of(a));
          assertFalse(io.github.openlogiclab.kafkapipeline.internal.FatalErrors.isFatal(a));

          RuntimeException c = new RuntimeException("c");
          TimeoutException timeout = new TimeoutException("expired");
          RuntimeException d = new RuntimeException("d", timeout);
          c.initCause(d);
          timeout.initCause(c); // c -> d -> timeout -> c
          assertEquals(WriteFailure.TRANSIENT, WriteFailure.of(c));
        });
  }

  @Test
  void otherErrors_areRecordFailures() {
    assertEquals(WriteFailure.RECORD, WriteFailure.of(new IllegalArgumentException("bad data")));
  }
}
