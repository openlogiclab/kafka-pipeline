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

import java.time.Duration;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class PipelineConfigValidationTest {

  private static PipelineConfig.Builder<String, String> builder(Properties p) {
    return PipelineConfig.<String, String>builder()
        .consumerProperties(p)
        .topics("t")
        .handler(r -> {})
        .concurrency(ThreadMode.PLATFORM, 1);
  }

  private static PipelineConfig.Builder<String, String> builder() {
    return builder(new Properties());
  }

  @Test
  void lifecycleHookWithBatchHandler_isRejected() {
    // Batch mode never calls the hook; accepting it would silently skip, say, deduplication.
    PipelineConfig.Builder<String, String> batch =
        PipelineConfig.<String, String>builder()
            .consumerProperties(new Properties())
            .topics("t")
            .concurrency(ThreadMode.PLATFORM, 1)
            .batchHandler((partition, records) -> {})
            .lifecycleHook(
                io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook.noOp());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, batch::build);
    assertTrue(e.getMessage().contains("lifecycleHook"), e.getMessage());

    assertDoesNotThrow(
        () ->
            builder()
                .lifecycleHook(
                    io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook.noOp())
                .build(),
        "per-record mode takes a hook");
  }

  @Test
  void commitIntervalMustBePositive() {
    assertThrows(
        IllegalArgumentException.class, () -> builder().commitInterval(Duration.ZERO).build());
    assertThrows(NullPointerException.class, () -> builder().commitInterval(null).build());
  }

  @Test
  void durationsTooLongToConvertToNanoseconds_areRejectedAtBuild() {
    // Natural ways to say "never" overflow nanoseconds and used to fail later, at construction or
    // in the middle of an outage.
    Duration forever = java.time.temporal.ChronoUnit.FOREVER.getDuration();
    Duration millionDays = Duration.ofDays(1_000_000);
    for (Duration d : new Duration[] {forever, millionDays}) {
      assertThrows(IllegalArgumentException.class, () -> builder().processingTimeout(d).build());
      assertThrows(IllegalArgumentException.class, () -> builder().writeOutageTimeout(d).build());
      assertThrows(IllegalArgumentException.class, () -> builder().commitInterval(d).build());
      assertThrows(IllegalArgumentException.class, () -> builder().shutdownTimeout(d).build());
    }
    assertDoesNotThrow(() -> builder().writeOutageTimeout(Duration.ofDays(3650)).build());
  }

  @Test
  void timeoutsMustNotBeNegative() {
    assertThrows(
        IllegalArgumentException.class,
        () -> builder().shutdownTimeout(Duration.ofSeconds(-1)).build());
    assertThrows(
        IllegalArgumentException.class, () -> builder().pollTimeout(Duration.ofMillis(-1)).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> builder().processingTimeout(Duration.ofMillis(-1)).build());
    assertEquals(Duration.ZERO, builder().build().processingTimeout(), "disabled by default");
  }

  @Test
  void drainTimeoutMustStayBelowMaxPollInterval() {
    Properties p = new Properties();
    p.put("max.poll.interval.ms", "10000");
    assertThrows(
        IllegalArgumentException.class,
        () -> builder(p).drainTimeout(Duration.ofSeconds(10)).build());
    assertDoesNotThrow(() -> builder(p).drainTimeout(Duration.ofSeconds(9)).build());
    // Kafka's default max.poll.interval.ms is 5 minutes.
    assertThrows(
        IllegalArgumentException.class,
        () -> builder().drainTimeout(Duration.ofMinutes(5)).build());
  }
}
