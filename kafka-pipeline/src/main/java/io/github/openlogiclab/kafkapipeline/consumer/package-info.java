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
 * Everything that drives the Kafka consumer, all on the single poll thread: the {@link
 * io.github.openlogiclab.kafkapipeline.consumer.PollLoop}, pausing and resuming partitions ({@link
 * io.github.openlogiclab.kafkapipeline.consumer.PartitionPauseController}), and rebalance handling
 * ({@link io.github.openlogiclab.kafkapipeline.consumer.PipelineRebalanceListener}).
 *
 * <p>Internal: not exported from the module.
 */
package io.github.openlogiclab.kafkapipeline.consumer;
