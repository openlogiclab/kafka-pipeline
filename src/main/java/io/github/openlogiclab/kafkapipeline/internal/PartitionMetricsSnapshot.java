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
package io.github.openlogiclab.kafkapipeline.internal;

import io.github.openlogiclab.kafkapipeline.metrics.PartitionMetrics;

/**
 * The only {@link PartitionMetrics} implementation.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 *
 * @param lag see {@link PartitionMetrics#lag()}
 * @param failures see {@link PartitionMetrics#failures()}
 * @param commitStallMillis see {@link PartitionMetrics#commitStallMillis()}
 * @param windowSize see {@link PartitionMetrics#windowSize()}
 * @param windowCapacity see {@link PartitionMetrics#windowCapacity()}
 */
public record PartitionMetricsSnapshot(
    long lag, int failures, long commitStallMillis, int windowSize, int windowCapacity)
    implements PartitionMetrics {}
