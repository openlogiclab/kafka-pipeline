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
package io.github.openlogiclab.kafkapipeline.offset;

/**
 * Central source of truth for record lifecycle and offset commit safety.
 *
 * <p>Every record flows through: REGISTERED → IN_PROGRESS → DONE. The tracker ensures that {@link
 * #getCommittableOffset} never returns an offset beyond contiguous completed records, preventing
 * silent data loss on crash/rebalance.
 *
 * <p>Uses a sliding window approach that supports concurrent out-of-order processing per partition.
 *
 * <p>Each component depends only on the role it uses: {@link ProcessingProgress} (workers), {@link
 * CommitSource} (committer), {@link PartitionLifecycle} (rebalance listener) and {@link
 * OffsetStats} (metrics and flow control).
 *
 * <p>All methods are thread-safe. Offset values follow Kafka commit semantics: committing N means
 * "next poll starts from offset N".
 */
public sealed interface OffsetTracker
    extends ProcessingProgress, CommitSource, PartitionLifecycle, OffsetStats
    permits UnorderedOffsetTracker {}
