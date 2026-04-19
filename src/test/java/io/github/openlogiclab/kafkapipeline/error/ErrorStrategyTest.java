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

import java.time.Duration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ErrorStrategyTest {

  // ── Factory Methods ──────────────────────────────────────────

  @Nested
  class Factories {

    @Test
    void failFast_noRetriesFailPartition() {
      ErrorStrategy<String, String> s = ErrorStrategy.failFast();
      assertEquals(0, s.maxRetries());
      assertEquals(Duration.ZERO, s.retryBackoff());
      assertFalse(s.hasDlq());
      assertEquals(Fallback.FAIL_PARTITION, s.fallback());
    }

    @Test
    void skipOnError_noRetriesSkip() {
      ErrorStrategy<String, String> s = ErrorStrategy.skipOnError();
      assertEquals(0, s.maxRetries());
      assertFalse(s.hasDlq());
      assertEquals(Fallback.SKIP, s.fallback());
    }
  }

  // ── Validation ───────────────────────────────────────────────

  @Nested
  class Validation {

    @Test
    void negativeRetries_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () -> new ErrorStrategy<>(-1, Duration.ZERO, false, Duration.ZERO, null, Fallback.SKIP));
    }

    @Test
    void zeroRetries_ok() {
      assertDoesNotThrow(
          () -> new ErrorStrategy<>(0, Duration.ZERO, false, Duration.ZERO, null, Fallback.SKIP));
    }

    @Test
    void negativeBackoff_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ErrorStrategy<>(
                  3, Duration.ofMillis(-1), false, Duration.ZERO, null, Fallback.SKIP));
    }

    @Test
    void negativeMaxBackoff_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new ErrorStrategy<>(
                  3, Duration.ofSeconds(1), true, Duration.ofMillis(-1), null, Fallback.SKIP));
    }
  }

  // ── hasDlq ───────────────────────────────────────────────────

  @Nested
  class HasDlq {

    @Test
    void noDlq_returnsFalse() {
      ErrorStrategy<String, String> s = ErrorStrategy.failFast();
      assertFalse(s.hasDlq());
    }

    @Test
    void withDlq_returnsTrue() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder().dlqHandler((record, error) -> {}).build();
      assertTrue(s.hasDlq());
    }
  }

  // ── Backoff Calculation ──────────────────────────────────────

  @Nested
  class BackoffCalculation {

    @Test
    void attempt0_returnsBaseBackoff() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder().retryBackoff(Duration.ofMillis(500)).build();
      assertEquals(Duration.ofMillis(500), s.backoffForAttempt(0));
    }

    @Test
    void fixedBackoff_sameForAllAttempts() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofMillis(200))
              .exponentialBackoff(false)
              .build();

      assertEquals(Duration.ofMillis(200), s.backoffForAttempt(1));
      assertEquals(Duration.ofMillis(200), s.backoffForAttempt(2));
      assertEquals(Duration.ofMillis(200), s.backoffForAttempt(5));
    }

    @Test
    void exponentialBackoff_doubles() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofMillis(100))
              .exponentialBackoff(true)
              .maxBackoff(Duration.ofMinutes(10))
              .build();

      assertEquals(Duration.ofMillis(200), s.backoffForAttempt(1));
      assertEquals(Duration.ofMillis(400), s.backoffForAttempt(2));
      assertEquals(Duration.ofMillis(800), s.backoffForAttempt(3));
    }

    @Test
    void exponentialBackoff_cappedAtMaxBackoff() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofMillis(100))
              .exponentialBackoff(true)
              .maxBackoff(Duration.ofMillis(500))
              .build();

      assertEquals(Duration.ofMillis(200), s.backoffForAttempt(1));
      assertEquals(Duration.ofMillis(400), s.backoffForAttempt(2));
      assertEquals(Duration.ofMillis(500), s.backoffForAttempt(3));
      assertEquals(Duration.ofMillis(500), s.backoffForAttempt(10));
    }

    @Test
    void exponentialBackoff_zeroMaxBackoff_noCap() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofMillis(100))
              .exponentialBackoff(true)
              .maxBackoff(Duration.ZERO)
              .build();

      assertEquals(Duration.ofMillis(200), s.backoffForAttempt(1));
      assertEquals(Duration.ofMillis(400), s.backoffForAttempt(2));
      assertEquals(Duration.ofMillis(3200), s.backoffForAttempt(5));
    }

    @Test
    void zeroBackoff_alwaysZero() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ZERO)
              .exponentialBackoff(true)
              .build();

      assertEquals(Duration.ZERO, s.backoffForAttempt(0));
      assertEquals(Duration.ZERO, s.backoffForAttempt(1));
      assertEquals(Duration.ZERO, s.backoffForAttempt(5));
    }

    @Test
    void negativeAttempt_returnsBaseBackoff() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofMillis(100))
              .exponentialBackoff(true)
              .build();

      assertEquals(Duration.ofMillis(100), s.backoffForAttempt(-1));
    }

    @Test
    void highAttempt_doesNotOverflow() {
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .retryBackoff(Duration.ofMillis(100))
              .exponentialBackoff(true)
              .maxBackoff(Duration.ofHours(1))
              .build();

      assertDoesNotThrow(() -> s.backoffForAttempt(50));
    }
  }

  // ── Builder ──────────────────────────────────────────────────

  @Nested
  class BuilderTest {

    @Test
    void builder_defaults() {
      ErrorStrategy<String, String> s = ErrorStrategy.<String, String>builder().build();
      assertEquals(0, s.maxRetries());
      assertEquals(Duration.ofSeconds(1), s.retryBackoff());
      assertFalse(s.exponentialBackoff());
      assertEquals(Duration.ofMinutes(1), s.maxBackoff());
      assertNull(s.dlqHandler());
      assertEquals(Fallback.FAIL_PARTITION, s.fallback());
    }

    @Test
    void builder_allFields() {
      DLQHandler<String, String> dlq = (record, error) -> {};
      ErrorStrategy<String, String> s =
          ErrorStrategy.<String, String>builder()
              .maxRetries(5)
              .retryBackoff(Duration.ofMillis(200))
              .exponentialBackoff(true)
              .maxBackoff(Duration.ofSeconds(30))
              .dlqHandler(dlq)
              .fallback(Fallback.SKIP)
              .build();

      assertEquals(5, s.maxRetries());
      assertEquals(Duration.ofMillis(200), s.retryBackoff());
      assertTrue(s.exponentialBackoff());
      assertEquals(Duration.ofSeconds(30), s.maxBackoff());
      assertSame(dlq, s.dlqHandler());
      assertEquals(Fallback.SKIP, s.fallback());
    }

    @Test
    void builder_fluent() {
      var b =
          ErrorStrategy.<String, String>builder()
              .maxRetries(3)
              .retryBackoff(Duration.ofMillis(100))
              .fallback(Fallback.SKIP);

      assertInstanceOf(ErrorStrategy.Builder.class, b);
    }
  }
}
