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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Per-key FIFO: at most one record per key is in flight, and records of a key are handed out in
 * offset order. Records of different keys are independent. Records with a null key are unordered.
 *
 * <p>All state is guarded by one lock per partition; the poll thread offers, workers poll and
 * release.
 */
final class KeyedLaneQueue<K, V> implements LaneQueue<K, V> {

  /**
   * Which keys of a partition are in flight, shared by every queue the partition gets. A record
   * still running from before a revoke keeps its key busy in the queue created when the partition
   * is assigned back, so the redelivered records of that key wait for it.
   */
  static final class KeyGuard {
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<Object> busy = new HashSet<>();
    private KeyedLaneQueue<?, ?> current; // guarded by lock

    /** True if no key of the partition is in flight. */
    boolean idle() {
      lock.lock();
      try {
        return busy.isEmpty();
      } finally {
        lock.unlock();
      }
    }
  }

  private final KeyGuard guard;
  private final ReentrantLock lock;
  private final int capacity;
  // Waiting records per key. A key is present while it has waiting records.
  private final Map<Object, ArrayDeque<ConsumerRecord<K, V>>> waiting = new HashMap<>();
  // Keys with waiting records and nothing in flight, in the order they became ready.
  private final ArrayDeque<Object> readyKeys = new ArrayDeque<>();
  // Keys with a record in flight, in this queue or an earlier one of the same partition.
  private final Set<Object> busy;
  private final ArrayDeque<ConsumerRecord<K, V>> unkeyed = new ArrayDeque<>();
  private int size;

  KeyedLaneQueue(int capacity) {
    this(capacity, new KeyGuard());
  }

  /** Creates the partition's current queue, sharing in-flight keys with its earlier queues. */
  KeyedLaneQueue(int capacity, KeyGuard guard) {
    this.capacity = capacity;
    this.guard = guard;
    this.lock = guard.lock;
    this.busy = guard.busy;
    lock.lock();
    try {
      guard.current = this;
    } finally {
      lock.unlock();
    }
  }

  /** byte[] keys compare by identity; wrap them so equal contents mean the same key. */
  static Object keyOf(ConsumerRecord<?, ?> record) {
    Object key = record.key();
    return key instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : key;
  }

  @Override
  public boolean offer(ConsumerRecord<K, V> record) {
    lock.lock();
    try {
      if (size >= capacity) return false;
      Object key = keyOf(record);
      if (key == null) {
        unkeyed.addLast(record);
      } else {
        ArrayDeque<ConsumerRecord<K, V>> queue = waiting.get(key);
        if (queue == null) {
          queue = new ArrayDeque<>();
          waiting.put(key, queue);
          if (!busy.contains(key)) readyKeys.addLast(key);
        }
        queue.addLast(record);
      }
      size++;
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ConsumerRecord<K, V> poll() {
    lock.lock();
    try {
      Object key = readyKeys.peekFirst();
      ConsumerRecord<K, V> keyedHead = key != null ? waiting.get(key).peekFirst() : null;
      ConsumerRecord<K, V> unkeyedHead = unkeyed.peekFirst();
      if (keyedHead == null && unkeyedHead == null) return null;

      // Prefer the lower offset so neither keyed nor unkeyed records starve.
      if (keyedHead == null || (unkeyedHead != null && unkeyedHead.offset() < keyedHead.offset())) {
        size--;
        return unkeyed.pollFirst();
      }
      readyKeys.pollFirst();
      ArrayDeque<ConsumerRecord<K, V>> queue = waiting.get(key);
      ConsumerRecord<K, V> record = queue.pollFirst();
      if (queue.isEmpty()) waiting.remove(key);
      busy.add(key);
      size--;
      return record;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void release(ConsumerRecord<K, V> record) {
    Object key = keyOf(record);
    if (key == null) return;
    lock.lock();
    try {
      busy.remove(key);
      // The record may come from an earlier queue of the partition; its key's next records wait
      // in the current one.
      KeyedLaneQueue<?, ?> current = guard.current;
      if (current.waiting.containsKey(key)) current.readyKeys.addLast(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean hasReady() {
    lock.lock();
    try {
      return !readyKeys.isEmpty() || !unkeyed.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return size;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int remainingCapacity() {
    lock.lock();
    try {
      return capacity - size;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<ConsumerRecord<K, V>> drain() {
    lock.lock();
    try {
      List<ConsumerRecord<K, V>> drained = new ArrayList<>(size);
      for (ArrayDeque<ConsumerRecord<K, V>> queue : waiting.values()) drained.addAll(queue);
      drained.addAll(unkeyed);
      waiting.clear();
      readyKeys.clear();
      unkeyed.clear();
      size = 0;
      return drained;
    } finally {
      lock.unlock();
    }
  }
}
