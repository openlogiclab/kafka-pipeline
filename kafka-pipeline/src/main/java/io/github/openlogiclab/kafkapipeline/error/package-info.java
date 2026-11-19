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
 * What happens when processing fails: {@link
 * io.github.openlogiclab.kafkapipeline.error.ErrorStrategy} retries a record, then sends it to the
 * {@link io.github.openlogiclab.kafkapipeline.error.DLQHandler} or drops it ({@link
 * io.github.openlogiclab.kafkapipeline.error.Fallback}), calling the {@link
 * io.github.openlogiclab.kafkapipeline.error.FinalFailureHandler} for dropped records. A failed
 * record is always acked in the end, so it never blocks its partition.
 *
 * <p>{@link io.github.openlogiclab.kafkapipeline.error.KafkaDLQHandler} writes dead letters to a
 * Kafka topic.
 *
 * <p>Public API.
 */
package io.github.openlogiclab.kafkapipeline.error;
