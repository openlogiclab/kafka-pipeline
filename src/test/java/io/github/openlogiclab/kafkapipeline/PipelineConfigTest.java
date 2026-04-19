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
import io.github.openlogiclab.kafkapipeline.error.ErrorStrategy;
import io.github.openlogiclab.kafkapipeline.error.Fallback;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PipelineConfigTest {

  private Properties props() {
    Properties p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    return p;
  }

  private PipelineConfig.Builder<String, String> minimalBuilder() {
    return PipelineConfig.<String, String>builder()
        .consumerProperties(props())
        .topics("test-topic")
        .handler(record -> {})
        .concurrency(ThreadMode.PLATFORM, 4);
  }

  // ── Builder Required Fields ──────────────────────────────────

  @Nested
  class RequiredFields {

    @Test
    void missingConsumerProperties_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .topics("t")
                  .handler(r -> {})
                  .concurrency(ThreadMode.PLATFORM, 4)
                  .build());
    }

    @Test
    void missingTopics_throws() {
      assertThrows(
          NullPointerException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .handler(r -> {})
                  .concurrency(ThreadMode.PLATFORM, 4)
                  .build());
    }

    @Test
    void missingHandler_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics("t")
                  .concurrency(ThreadMode.PLATFORM, 4)
                  .build());
    }

    @Test
    void missingConcurrency_throws() {
      assertThrows(
          IllegalStateException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics("t")
                  .handler(r -> {})
                  .build());
    }

    @Test
    void emptyTopics_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics(List.of())
                  .handler(r -> {})
                  .concurrency(ThreadMode.PLATFORM, 4)
                  .build());
    }

    @Test
    void zeroConcurrency_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics("t")
                  .handler(r -> {})
                  .concurrency(ThreadMode.PLATFORM, 0)
                  .build());
    }

    @Test
    void negativeConcurrency_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics("t")
                  .handler(r -> {})
                  .concurrency(ThreadMode.PLATFORM, -1)
                  .build());
    }

    @Test
    void bothHandlers_throws() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics("t")
                  .handler(r -> {})
                  .batchHandler((tp, records) -> {})
                  .concurrency(ThreadMode.PLATFORM, 4)
                  .build());
    }

    @Test
    void batchHandlerOnly_succeeds() {
      PipelineConfig<String, String> config =
          PipelineConfig.<String, String>builder()
              .consumerProperties(props())
              .topics("t")
              .batchHandler((tp, records) -> {})
              .concurrency(ThreadMode.VIRTUAL, 8)
              .build();
      assertTrue(config.isBatchMode());
      assertNull(config.handler());
      assertNotNull(config.batchHandler());
    }
  }

  // ── Thread Mode ──────────────────────────────────────────────

  @Nested
  class ConcurrencyConfig {

    @Test
    void platformThreads() {
      PipelineConfig<String, String> config =
          minimalBuilder().concurrency(ThreadMode.PLATFORM, 8).build();
      assertEquals(ThreadMode.PLATFORM, config.threadMode());
      assertEquals(8, config.concurrency());
    }

    @Test
    void virtualThreads() {
      PipelineConfig<String, String> config =
          minimalBuilder().concurrency(ThreadMode.VIRTUAL, 200).build();
      assertEquals(ThreadMode.VIRTUAL, config.threadMode());
      assertEquals(200, config.concurrency());
    }

    @Test
    void nullThreadMode_throwsOnBuild() {
      assertThrows(
          IllegalStateException.class,
          () ->
              PipelineConfig.<String, String>builder()
                  .consumerProperties(props())
                  .topics("t")
                  .handler(r -> {})
                  .build());
    }
  }

  // ── Defaults ─────────────────────────────────────────────────

  @Nested
  class Defaults {

    @Test
    void defaultBackpressure() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(BackpressureConfig.defaults(), config.backpressure());
    }

    @Test
    void defaultErrorStrategy_isFailFast() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(0, config.errorStrategy().maxRetries());
      assertEquals(Fallback.FAIL_PARTITION, config.errorStrategy().fallback());
    }

    @Test
    void defaultLifecycleHook_isNoOp() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertNotNull(config.lifecycleHook());
    }

    @Test
    void defaultCommitInterval() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(Duration.ofSeconds(5), config.commitInterval());
    }

    @Test
    void defaultDrainTimeout() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(Duration.ofSeconds(30), config.drainTimeout());
    }

    @Test
    void defaultShutdownTimeout() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(Duration.ofSeconds(30), config.shutdownTimeout());
    }

    @Test
    void defaultPollTimeout() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(Duration.ofMillis(100), config.pollTimeout());
    }

    @Test
    void defaultDispatchQueueCapacity() {
      PipelineConfig<String, String> config = minimalBuilder().build();
      assertEquals(500, config.dispatchQueueCapacity());
    }
  }

  // ── Custom Values ────────────────────────────────────────────

  @Nested
  class CustomValues {

    @Test
    void topicsVarargs() {
      PipelineConfig<String, String> config = minimalBuilder().topics("a", "b", "c").build();
      assertEquals(List.of("a", "b", "c"), config.topics());
    }

    @Test
    void topicsList() {
      PipelineConfig<String, String> config = minimalBuilder().topics(List.of("x", "y")).build();
      assertEquals(List.of("x", "y"), config.topics());
    }

    @Test
    void customBackpressure() {
      BackpressureConfig bp =
          BackpressureConfig.builder()
              .highWatermark(500)
              .lowWatermark(300)
              .criticalThreshold(1000)
              .build();

      PipelineConfig<String, String> config = minimalBuilder().backpressure(bp).build();
      assertEquals(500, config.backpressure().highWatermark());
    }

    @Test
    void customErrorStrategy() {
      ErrorStrategy<String, String> es =
          ErrorStrategy.<String, String>builder().maxRetries(5).fallback(Fallback.SKIP).build();

      PipelineConfig<String, String> config = minimalBuilder().errorStrategy(es).build();
      assertEquals(5, config.errorStrategy().maxRetries());
      assertEquals(Fallback.SKIP, config.errorStrategy().fallback());
    }

    @Test
    void customLifecycleHook() {
      ProcessingLifecycleHook<String, String> hook = new ProcessingLifecycleHook<>() {};
      PipelineConfig<String, String> config = minimalBuilder().lifecycleHook(hook).build();
      assertSame(hook, config.lifecycleHook());
    }

    @Test
    void customTimeouts() {
      PipelineConfig<String, String> config =
          minimalBuilder()
              .commitInterval(Duration.ofSeconds(10))
              .drainTimeout(Duration.ofSeconds(60))
              .shutdownTimeout(Duration.ofSeconds(45))
              .pollTimeout(Duration.ofMillis(200))
              .build();

      assertEquals(Duration.ofSeconds(10), config.commitInterval());
      assertEquals(Duration.ofSeconds(60), config.drainTimeout());
      assertEquals(Duration.ofSeconds(45), config.shutdownTimeout());
      assertEquals(Duration.ofMillis(200), config.pollTimeout());
    }

    @Test
    void customDispatchQueueCapacity() {
      PipelineConfig<String, String> config = minimalBuilder().dispatchQueueCapacity(5000).build();
      assertEquals(5000, config.dispatchQueueCapacity());
    }

    @Test
    void zeroDispatchQueueCapacity_throws() {
      assertThrows(
          IllegalArgumentException.class, () -> minimalBuilder().dispatchQueueCapacity(0).build());
    }
  }

  // ── Static Builder Shortcut ──────────────────────────────────

  @Nested
  class StaticBuilder {

    @Test
    void kafkaPipeline_builderShortcut() {
      PipelineConfig.Builder<String, String> b = KafkaPipeline.builder();
      assertNotNull(b);
    }
  }
}
