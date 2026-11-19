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

import org.junit.jupiter.api.Test;

class BackpressureConfigsTest {

  @Test
  void recordConfig() {
    RecordBackpressureConfig d = RecordBackpressureConfig.defaults();
    assertEquals(10_000, d.highWatermark());
    assertEquals(6_000, d.lowWatermark());
    assertTrue(d.enabled());
    assertFalse(RecordBackpressureConfig.disabled().enabled());
    assertThrows(
        IllegalArgumentException.class,
        () -> RecordBackpressureConfig.builder().highWatermark(5).lowWatermark(5).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> RecordBackpressureConfig.builder().lowWatermark(0).build());
  }

  @Test
  void byteConfig() {
    ByteBackpressureConfig b = ByteBackpressureConfig.builder().build();
    assertEquals(256L << 20, b.highWatermarkBytes());
    assertEquals(128L << 20, b.lowWatermarkBytes());
    assertFalse(ByteBackpressureConfig.disabled().enabled());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ByteBackpressureConfig.builder().highWatermarkBytes(10).lowWatermarkBytes(10).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> ByteBackpressureConfig.builder().lowWatermarkBytes(0).build());
  }

  @Test
  void heapConfig() {
    HeapBackpressureConfig h = HeapBackpressureConfig.builder().build();
    assertEquals(0.7, h.highWatermark());
    assertEquals(0.5, h.lowWatermark());
    assertFalse(HeapBackpressureConfig.disabled().enabled());
    assertThrows(
        IllegalArgumentException.class,
        () -> HeapBackpressureConfig.builder().lowWatermark(0).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> HeapBackpressureConfig.builder().highWatermark(1.1).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> HeapBackpressureConfig.builder().highWatermark(0.5).lowWatermark(0.6).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> HeapBackpressureConfig.builder().highWatermark(Double.NaN).build());
  }
}
