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
package io.github.openlogiclab.kafkapipeline.worker;

import io.github.openlogiclab.kafkapipeline.ThreadMode;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Abstraction over how polled records are distributed and processed.
 *
 * <p>Implementations encapsulate offset tracking registration, in-flight counting, error handling,
 * and the processing model (per-record vs. batch). The pipeline poll loop simply calls {@link
 * #dispatch(ConsumerRecords)} without knowing which mode is active.
 *
 * <p>Shared lifecycle (executor creation, stop, running flag) lives here; subclasses only implement
 * {@link #start()} and {@link #dispatch(ConsumerRecords)}.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public abstract sealed class WorkerPool<K, V> permits SingleRecordWorkerPool, BatchWorkerPool {

  private static final System.Logger logger = System.getLogger(WorkerPool.class.getName());

  /** The executor for running worker tasks. */
  protected final ExecutorService executor;

  /** Whether the pool is running. */
  protected final AtomicBoolean running = new AtomicBoolean(false);

  /** Number of worker threads. */
  protected final int concurrency;

  /** Thread mode (platform or virtual). */
  protected final ThreadMode threadMode;

  /**
   * Creates a new worker pool.
   *
   * @param concurrency number of worker threads
   * @param threadMode platform or virtual threads
   * @param threadNamePrefix prefix for thread names
   * @param taskQueueCapacity max queued tasks for PLATFORM mode; 0 = unbounded
   */
  protected WorkerPool(
      int concurrency, ThreadMode threadMode, String threadNamePrefix, int taskQueueCapacity) {
    if (concurrency <= 0) {
      throw new IllegalArgumentException("concurrency must be positive, got " + concurrency);
    }
    this.concurrency = concurrency;
    this.threadMode = threadMode;
    this.executor = createExecutor(threadMode, concurrency, threadNamePrefix, taskQueueCapacity);
  }

  /** Starts the worker threads. */
  public abstract void start();

  /**
   * Dispatches records to workers for processing.
   *
   * <p>If a partition's offset window has no room, that partition's remaining records are not
   * dispatched. The returned map gives, for each such partition, the offset of the first record
   * that was not dispatched; the caller must seek the consumer back to it so nothing is skipped.
   *
   * @param records the records to process
   * @return first undispatched offset per partition whose window was full (empty if all records
   *     were dispatched)
   */
  public abstract Map<TopicPartition, Long> dispatch(ConsumerRecords<K, V> records);

  /**
   * Returns whether a partition that was paused because its records could not be queued may resume
   * fetching. Called from the poll thread.
   *
   * @param tp the partition
   * @return true once there is room again
   */
  public abstract boolean canResume(TopicPartition tp);

  /**
   * Returns whether the bounded task queue (PLATFORM threads with a task queue capacity) has room
   * for one more task. Always true for unbounded executors. The poll thread is the only submitter,
   * so a {@code true} result stays true until it submits.
   */
  protected boolean hasTaskQueueRoom() {
    return !(executor instanceof ThreadPoolExecutor tpe) || tpe.getQueue().remainingCapacity() > 0;
  }

  /** Returns whether the bounded task queue has drained to at most half its capacity. */
  protected boolean taskQueueDrained() {
    if (!(executor instanceof ThreadPoolExecutor tpe)) return true;
    var queue = tpe.getQueue();
    int capacity = queue.size() + queue.remainingCapacity();
    return queue.size() <= capacity / 2;
  }

  // Rate limit for the "task queue full" warning: at most one log line per interval.
  private static final long QUEUE_FULL_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
  private final AtomicLong lastQueueFullLogNanos =
      new AtomicLong(System.nanoTime() - QUEUE_FULL_LOG_INTERVAL_NANOS);
  private final AtomicLong queueFullCount = new AtomicLong();

  /** Logs a rate-limited warning that a partition was paused because the task queue was full. */
  protected void reportTaskQueueFull(TopicPartition tp) {
    long count = queueFullCount.incrementAndGet();
    long now = System.nanoTime();
    long last = lastQueueFullLogNanos.get();
    if (now - last >= QUEUE_FULL_LOG_INTERVAL_NANOS
        && lastQueueFullLogNanos.compareAndSet(last, now)) {
      queueFullCount.addAndGet(-count);
      logger.log(
          System.Logger.Level.WARNING,
          "Executor task queue full, pausing {0} until workers catch up ({1} times since last"
              + " warning) — consider more concurrency or a larger dispatchQueueCapacity",
          tp,
          count);
    }
  }

  /**
   * Stops the worker pool and waits for termination.
   *
   * @param timeoutMs max time to wait in milliseconds
   */
  public void stop(long timeoutMs) {
    running.set(false);
    executor.shutdown();
    try {
      if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
        logger.log(
            System.Logger.Level.WARNING,
            "WorkerPool did not terminate within {0}ms, forcing shutdown",
            timeoutMs);
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
  }

  /**
   * Returns whether the pool is running.
   *
   * @return true if running
   */
  public boolean isRunning() {
    return running.get();
  }

  private static ExecutorService createExecutor(
      ThreadMode mode, int concurrency, String namePrefix, int taskQueueCapacity) {
    return switch (mode) {
      case PLATFORM -> {
        var factory =
            (java.util.concurrent.ThreadFactory)
                r -> {
                  Thread t = new Thread(r);
                  t.setName(namePrefix + t.threadId());
                  t.setDaemon(true);
                  return t;
                };
        if (taskQueueCapacity > 0) {
          var queue = new LinkedBlockingQueue<Runnable>(taskQueueCapacity);
          yield new ThreadPoolExecutor(
              concurrency,
              concurrency,
              0L,
              TimeUnit.MILLISECONDS,
              queue,
              factory,
              (task, pool) -> {
                if (pool.isShutdown()) return;
                // Unreachable while dispatch() checks hasTaskQueueRoom() first; fail loudly rather
                // than blocking the poll thread.
                throw new RejectedExecutionException(
                    "Task queue full (capacity=" + taskQueueCapacity + ")");
              });
        } else {
          yield Executors.newFixedThreadPool(concurrency, factory);
        }
      }
      case VIRTUAL ->
          Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(namePrefix, 0).factory());
    };
  }
}
