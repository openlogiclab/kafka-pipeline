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

/**
 * Ordering guarantee for records of the same partition.
 *
 * <p>Kafka only orders records within a partition. This setting decides how much of that order the
 * pipeline keeps while processing records concurrently.
 */
public enum ProcessingOrder {

  /**
   * No ordering: records of a partition are processed in parallel and may finish in any order.
   * Highest throughput. Offsets are still committed safely (only past records that are done).
   */
  UNORDERED,

  /**
   * Records with the same key are processed one at a time, in offset order; records with different
   * keys run in parallel. A record waiting for an earlier record of its key does not block other
   * keys. Records with a {@code null} key are not ordered. Retries of a record hold back later
   * records of its key.
   *
   * <p>In batch mode this behaves like {@link #PARTITION}, because a batch mixes keys.
   */
  KEY,

  /**
   * Records of a partition are processed strictly one at a time, in offset order. Parallelism comes
   * only from having several partitions. In batch mode, at most one batch per partition is
   * processed at a time.
   */
  PARTITION
}
