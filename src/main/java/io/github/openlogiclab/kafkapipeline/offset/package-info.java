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
 * Which offsets are done and how far each partition can be committed. {@link
 * io.github.openlogiclab.kafkapipeline.offset.SlidingWindowOffsetTracker} keeps one sliding window
 * per partition that accepts acks in any order. Callers depend on the narrow role interfaces
 * ({@code ProcessingProgress} for workers, {@code CommitSource} for the committer, {@code
 * PartitionLifecycle} for rebalances, {@code OffsetStats} for metrics and pausing). Thread-safe.
 *
 * <p>Internal: not exported from the module.
 */
package io.github.openlogiclab.kafkapipeline.offset;
