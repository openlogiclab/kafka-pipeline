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
 * State of one offset in a {@link PartitionWindow}. An offset moves {@code REGISTERED → IN_PROGRESS
 * → DONE}; a record that was dropped after its retries is also {@code DONE}, because it is finished
 * with and may be committed.
 */
enum OffsetStatus {
  /** Polled and queued, not started yet. */
  REGISTERED,
  /** A worker is processing it (or its sink write is in flight). */
  IN_PROGRESS,
  /** Finished: processed, dead-lettered or dropped. Committable once nothing before it is open. */
  DONE
}
