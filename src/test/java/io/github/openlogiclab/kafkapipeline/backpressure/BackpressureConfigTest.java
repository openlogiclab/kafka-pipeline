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

class BackpressureConfigTest {

  @Nested
  class Defaults {

    @Test
    void defaultsHaveSensibleValues() {
      BackpressureConfig config = BackpressureConfig.defaults();
      assertTrue(config.enabled());
      assertTrue(config.highWatermark() > config.lowWatermark());
      assertTrue(config.criticalThreshold() > config.highWatermark());
      assertEquals(10_000, config.highWatermark());
      assertEquals(6_000, config.lowWatermark());
      assertEquals(50_000, config.criticalThreshold());
    }

    @Test
    void disabledConfig_bypassesValidation() {
      BackpressureConfig config = BackpressureConfig.disabled();
      assertFalse(config.enabled());
    }
  }

  @Nested
  class Validation {

    @Test
    void lowWatermark_mustBePositive() {
      assertThrows(IllegalArgumentException.class, () -> new BackpressureConfig(100, 0, 200, true));
    }

    @Test
    void highWatermark_mustBeGreaterThanLow() {
      assertThrows(
          IllegalArgumentException.class, () -> new BackpressureConfig(100, 100, 200, true));
    }

    @Test
    void criticalThreshold_mustBeGreaterThanHigh() {
      assertThrows(
          IllegalArgumentException.class, () -> new BackpressureConfig(100, 50, 100, true));
    }

    @Test
    void validConfig_doesNotThrow() {
      assertDoesNotThrow(() -> new BackpressureConfig(100, 50, 200, true));
    }

    @Test
    void disabledConfig_skipsValidation() {
      assertDoesNotThrow(() -> new BackpressureConfig(0, 0, 0, false));
    }
  }

  @Nested
  class BuilderTest {

    @Test
    void builder_defaultValues() {
      BackpressureConfig config = BackpressureConfig.builder().build();
      assertEquals(10_000, config.highWatermark());
      assertEquals(6_000, config.lowWatermark());
      assertEquals(50_000, config.criticalThreshold());
      assertTrue(config.enabled());
    }

    @Test
    void builder_customValues() {
      BackpressureConfig config =
          BackpressureConfig.builder()
              .highWatermark(500)
              .lowWatermark(300)
              .criticalThreshold(1000)
              .build();

      assertEquals(500, config.highWatermark());
      assertEquals(300, config.lowWatermark());
      assertEquals(1000, config.criticalThreshold());
    }

    @Test
    void builder_disabled() {
      BackpressureConfig config = BackpressureConfig.builder().enabled(false).build();
      assertFalse(config.enabled());
    }
  }
}
