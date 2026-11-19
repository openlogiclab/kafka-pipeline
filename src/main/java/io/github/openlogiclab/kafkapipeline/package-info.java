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
/**
 * kafka-pipeline: a Kafka consumer that processes the records of each partition concurrently, keeps
 * order where you ask for it, applies global backpressure and commits only what is done.
 *
 * <p>Start with {@link io.github.openlogiclab.kafkapipeline.KafkaPipeline} and its builder ({@link
 * io.github.openlogiclab.kafkapipeline.PipelineConfig.Builder}). The processing modes are a {@code
 * handler} (side effects only), a {@code processor} with a {@code sink} (writes output, acked once
 * written), or a {@code batchHandler}. {@link io.github.openlogiclab.kafkapipeline.ProcessingOrder}
 * chooses the ordering and {@link io.github.openlogiclab.kafkapipeline.ThreadMode} the worker
 * threads.
 *
 * <p>Public API.
 */
package io.github.openlogiclab.kafkapipeline;
