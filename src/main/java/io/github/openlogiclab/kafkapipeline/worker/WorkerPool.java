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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecords;

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
   * @param records the records to process
   */
  public abstract void dispatch(ConsumerRecords<K, V> records);

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
                logger.log(
                    System.Logger.Level.WARNING,
                    "Executor task queue full (capacity={0}), caller thread will block "
                        + "— consider increasing concurrency or lowering throughput",
                    taskQueueCapacity);
                try {
                  pool.getQueue().put(task);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
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
