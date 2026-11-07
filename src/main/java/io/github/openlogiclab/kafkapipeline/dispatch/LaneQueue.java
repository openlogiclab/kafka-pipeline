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
package io.github.openlogiclab.kafkapipeline.dispatch;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Bounded queue of one partition's records waiting for a worker. */
interface LaneQueue<K, V> {

  /** Adds a record; returns false if the queue is full. */
  boolean offer(ConsumerRecord<K, V> record);

  /** Returns the next record a worker may start now, or null if none is ready. */
  ConsumerRecord<K, V> poll();

  /**
   * Signals that a record returned by {@link #poll} finished processing.
   *
   * @return true if finishing it may have made other records of the partition ready, so a waiting
   *     worker should check again
   */
  boolean release(ConsumerRecord<K, V> record);

  /** Whether {@link #poll} would currently return a record. */
  boolean hasReady();

  int size();

  int remainingCapacity();

  /** Removes and returns all queued records. */
  List<ConsumerRecord<K, V>> drain();
}
