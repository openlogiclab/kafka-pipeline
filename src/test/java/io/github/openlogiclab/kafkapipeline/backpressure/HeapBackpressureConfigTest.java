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
package io.github.openlogiclab.kafkapipeline.backpressure;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HeapBackpressureConfigTest {

  @Test
  void disabled_allowsZeroThresholds() {
    HeapBackpressureConfig cfg = HeapBackpressureConfig.disabled();
    assertFalse(cfg.enabled());
    assertEquals(0, cfg.throttleThreshold());
    assertEquals(0, cfg.resumeThreshold());
    assertEquals(0, cfg.criticalThreshold());
  }

  @Test
  void builderDefaults_producesValidConfig() {
    HeapBackpressureConfig cfg = HeapBackpressureConfig.builder().build();
    assertTrue(cfg.enabled());
    assertEquals(0.7, cfg.throttleThreshold());
    assertEquals(0.5, cfg.resumeThreshold());
    assertEquals(0.9, cfg.criticalThreshold());
  }

  @Test
  void customThresholds() {
    HeapBackpressureConfig cfg =
        HeapBackpressureConfig.builder()
            .resumeThreshold(0.4)
            .throttleThreshold(0.6)
            .criticalThreshold(0.85)
            .build();
    assertTrue(cfg.enabled());
    assertEquals(0.6, cfg.throttleThreshold());
    assertEquals(0.4, cfg.resumeThreshold());
    assertEquals(0.85, cfg.criticalThreshold());
  }

  @Nested
  class Validation {

    @Test
    void resumeThreshold_mustBePositive() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              HeapBackpressureConfig.builder()
                  .resumeThreshold(0.0)
                  .throttleThreshold(0.7)
                  .criticalThreshold(0.9)
                  .build());
    }

    @Test
    void resumeThreshold_mustBeLessThanOne() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              HeapBackpressureConfig.builder()
                  .resumeThreshold(1.0)
                  .throttleThreshold(1.1)
                  .criticalThreshold(1.2)
                  .build());
    }

    @Test
    void throttleThreshold_mustExceedResume() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              HeapBackpressureConfig.builder()
                  .resumeThreshold(0.7)
                  .throttleThreshold(0.7)
                  .criticalThreshold(0.9)
                  .build());
    }

    @Test
    void criticalThreshold_mustExceedThrottle() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              HeapBackpressureConfig.builder()
                  .resumeThreshold(0.3)
                  .throttleThreshold(0.7)
                  .criticalThreshold(0.7)
                  .build());
    }

    @Test
    void criticalThreshold_mustNotExceedOne() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              HeapBackpressureConfig.builder()
                  .resumeThreshold(0.5)
                  .throttleThreshold(0.7)
                  .criticalThreshold(1.1)
                  .build());
    }

    @Test
    void disabled_skipsValidation() {
      assertDoesNotThrow(
          () ->
              HeapBackpressureConfig.builder()
                  .enabled(false)
                  .resumeThreshold(0)
                  .throttleThreshold(0)
                  .criticalThreshold(0)
                  .build());
    }
  }
}
