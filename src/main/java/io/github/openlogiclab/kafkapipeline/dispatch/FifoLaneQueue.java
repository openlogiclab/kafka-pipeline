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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Records handed out in offset order; any number may be in flight (limited by the dispatcher). */
final class FifoLaneQueue<K, V> implements LaneQueue<K, V> {

  private final LinkedBlockingQueue<ConsumerRecord<K, V>> queue;

  FifoLaneQueue(int capacity) {
    this.queue = new LinkedBlockingQueue<>(capacity);
  }

  @Override
  public boolean offer(ConsumerRecord<K, V> record) {
    return queue.offer(record);
  }

  @Override
  public ConsumerRecord<K, V> poll() {
    return queue.poll();
  }

  @Override
  public void release(ConsumerRecord<K, V> record) {}

  @Override
  public boolean hasReady() {
    return !queue.isEmpty();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public List<ConsumerRecord<K, V>> drain() {
    List<ConsumerRecord<K, V>> drained = new ArrayList<>(queue.size());
    queue.drainTo(drained);
    return drained;
  }
}
