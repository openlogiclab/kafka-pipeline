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
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
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

  // Called when a worker hits an error the pipeline cannot recover from (OutOfMemoryError and
  // similar).
  private volatile Consumer<Throwable> fatalErrorHandler = t -> {};

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

  /**
   * Records that a partition's records from {@code offset} on were not dispatched, so the caller
   * seeks back to it. Allocates the map only on the first rejection of a poll.
   *
   * @param rejected the rejections so far ({@code Map.of()} when none)
   * @param tp the partition
   * @param offset the first offset not dispatched
   * @return the map holding the rejection
   */
  protected static Map<TopicPartition, Long> reject(
      Map<TopicPartition, Long> rejected, TopicPartition tp, long offset) {
    Map<TopicPartition, Long> result = rejected.isEmpty() ? new HashMap<>() : rejected;
    result.put(tp, offset);
    return result;
  }

  /** Starts the worker threads. */
  public abstract void start();

  /**
   * Sets the callback invoked when a worker hits an error the pipeline cannot safely continue
   * after, such as {@link OutOfMemoryError}. The record involved is left unacked so it is
   * redelivered.
   *
   * @param handler the callback
   */
  public void onFatalError(Consumer<Throwable> handler) {
    this.fatalErrorHandler = handler;
  }

  /**
   * Logs an unexpected throwable from a worker and escalates it if the JVM may be unusable.
   *
   * @param t the throwable
   */
  protected void reportWorkerError(Throwable t) {
    if (FatalErrors.isJvmFatal(t)) {
      logger.log(System.Logger.Level.ERROR, "Fatal error in worker, stopping the pipeline", t);
      fatalErrorHandler.accept(t);
    } else {
      logger.log(System.Logger.Level.ERROR, "Unexpected error in worker", t);
    }
  }

  /**
   * Whether the pool is stopping; interrupts mean shutdown only while this is true.
   *
   * @return true once {@link #stop} has been called
   */
  protected boolean stopping() {
    return !running.get();
  }

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
   * Whether the pool needs the partition to stay paused for a reason of its own (for example an
   * ordered batch still running). A per-partition limit, combined with global backpressure by the
   * pause controller.
   *
   * @param tp the partition
   * @return true while the partition must not be fetched
   */
  public boolean holdsPartition(TopicPartition tp) {
    return false;
  }

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

  private final AtomicLong lastDispatchFailureLogNanos =
      new AtomicLong(System.nanoTime() - QUEUE_FULL_LOG_INTERVAL_NANOS);

  /**
   * Logs (rate-limited) that a partition's records were sent back because registering failed.
   *
   * @param tp the partition
   * @param offset first offset sent back
   * @param e the failure
   */
  protected void reportDispatchFailure(TopicPartition tp, long offset, RuntimeException e) {
    long now = System.nanoTime();
    long last = lastDispatchFailureLogNanos.get();
    if (now - last >= QUEUE_FULL_LOG_INTERVAL_NANOS
        && lastDispatchFailureLogNanos.compareAndSet(last, now)) {
      logger.log(
          System.Logger.Level.WARNING,
          "Could not register {0} from offset {1}, fetching it again later: {2}",
          tp,
          offset,
          e.toString());
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
        ThreadFactory factory =
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
