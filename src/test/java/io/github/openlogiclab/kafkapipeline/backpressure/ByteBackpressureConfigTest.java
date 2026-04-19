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

class ByteBackpressureConfigTest {

  @Test
  void disabled_allowsZeroThresholds() {
    ByteBackpressureConfig cfg = ByteBackpressureConfig.disabled();
    assertFalse(cfg.enabled());
    assertEquals(0, cfg.highWatermarkBytes());
    assertEquals(0, cfg.lowWatermarkBytes());
    assertEquals(0, cfg.criticalThresholdBytes());
  }

  @Test
  void builderDefaults_producesValidConfig() {
    ByteBackpressureConfig cfg = ByteBackpressureConfig.builder().build();
    assertTrue(cfg.enabled());
    assertEquals(256L * 1024 * 1024, cfg.highWatermarkBytes());
    assertEquals(128L * 1024 * 1024, cfg.lowWatermarkBytes());
    assertEquals(512L * 1024 * 1024, cfg.criticalThresholdBytes());
  }

  @Test
  void customThresholds() {
    ByteBackpressureConfig cfg =
        ByteBackpressureConfig.builder()
            .lowWatermarkBytes(50_000)
            .highWatermarkBytes(100_000)
            .criticalThresholdBytes(200_000)
            .build();
    assertTrue(cfg.enabled());
    assertEquals(100_000, cfg.highWatermarkBytes());
    assertEquals(50_000, cfg.lowWatermarkBytes());
    assertEquals(200_000, cfg.criticalThresholdBytes());
  }

  @Nested
  class Validation {

    @Test
    void lowWatermark_mustBePositive() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ByteBackpressureConfig.builder()
                  .lowWatermarkBytes(0)
                  .highWatermarkBytes(100)
                  .criticalThresholdBytes(200)
                  .build());
    }

    @Test
    void highWatermark_mustExceedLow() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ByteBackpressureConfig.builder()
                  .lowWatermarkBytes(100)
                  .highWatermarkBytes(100)
                  .criticalThresholdBytes(200)
                  .build());
    }

    @Test
    void criticalThreshold_mustExceedHigh() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ByteBackpressureConfig.builder()
                  .lowWatermarkBytes(50)
                  .highWatermarkBytes(100)
                  .criticalThresholdBytes(100)
                  .build());
    }

    @Test
    void disabled_skipsValidation() {
      assertDoesNotThrow(
          () ->
              ByteBackpressureConfig.builder()
                  .enabled(false)
                  .lowWatermarkBytes(0)
                  .highWatermarkBytes(0)
                  .criticalThresholdBytes(0)
                  .build());
    }
  }
}
