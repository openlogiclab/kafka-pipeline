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

import java.util.OptionalLong;

/**
 * Result of draining a partition during rebalance or shutdown. Reports how many records completed
 * vs. were abandoned (still pending/in-progress at timeout).
 */
public record PartitionDrainResult(
    boolean allCompleted, int completedCount, int abandonedCount, OptionalLong committableOffset) {

  /**
   * The offset safe to commit for this partition, or empty if no progress was made. This value
   * follows Kafka semantics: committing N means "next poll starts from N".
   */
  @Override
  public OptionalLong committableOffset() {
    return committableOffset;
  }

  @Override
  public String toString() {
    return "PartitionDrainResult{allCompleted="
        + allCompleted
        + ", completed="
        + completedCount
        + ", abandoned="
        + abandonedCount
        + ", committableOffset="
        + committableOffset
        + "}";
  }
}
