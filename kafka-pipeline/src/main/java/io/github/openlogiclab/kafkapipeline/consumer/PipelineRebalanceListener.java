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
package io.github.openlogiclab.kafkapipeline.consumer;

import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.RebalanceMetrics;
import io.github.openlogiclab.kafkapipeline.internal.RecordSize;
import io.github.openlogiclab.kafkapipeline.internal.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.offset.PartitionDrainResult;
import io.github.openlogiclab.kafkapipeline.offset.PartitionLifecycle;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Handles Kafka consumer group rebalance events.
 *
 * <p>On revoke: drain in-flight records, commit final offsets, clean up state. On assign:
 * initialize offset tracking and dispatch queues for new partitions.
 */
public final class PipelineRebalanceListener implements ConsumerRebalanceListener {

  private static final System.Logger logger =
      System.getLogger(PipelineRebalanceListener.class.getName());

  private final PartitionLifecycle offsetTracker;
  private final RecordDispatcher<?, ?> dispatcher;
  private final Runnable commitSync;
  private final InFlightCounter counter;
  private final Consumer<?, ?> consumer;
  private final Duration drainTimeout;
  private final RebalanceMetrics metrics;
  private final ExecutorService drainExecutor;

  private static final long INIT_RETRY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static final long INIT_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
  // Total time one initialization pass may block the poll thread, kept well below the default
  // max.poll.interval.ms (5 minutes) however many partitions are assigned.
  private static final Duration INIT_BUDGET = Duration.ofSeconds(10);
  private static final Duration INIT_RETRY_BUDGET = Duration.ofSeconds(5);
  // Assigned partitions whose initialization failed; poll thread only.
  private final Set<TopicPartition> pendingInit = new HashSet<>();
  private final List<PartitionDrainListener> drainListeners = new CopyOnWriteArrayList<>();
  private final List<java.util.function.Consumer<Collection<TopicPartition>>> assignListeners =
      new CopyOnWriteArrayList<>();
  private long lastInitRetryNanos = System.nanoTime();
  private long lastInitWarnNanos = System.nanoTime();

  /**
   * Creates a rebalance listener.
   *
   * @param offsetTracker the offset tracker
   * @param dispatcher the record dispatcher
   * @param commitSync callback to perform sync commit
   * @param counter the in-flight counter
   * @param consumer the Kafka consumer
   * @param drainTimeout timeout for draining in-flight records
   * @param metrics the metrics this component records
   */
  public PipelineRebalanceListener(
      PartitionLifecycle offsetTracker,
      RecordDispatcher<?, ?> dispatcher,
      Runnable commitSync,
      InFlightCounter counter,
      Consumer<?, ?> consumer,
      Duration drainTimeout,
      RebalanceMetrics metrics) {
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.commitSync = commitSync;
    this.counter = counter;
    this.consumer = consumer;
    this.drainTimeout = Objects.requireNonNull(drainTimeout, "drainTimeout");
    this.metrics = metrics;
    this.drainExecutor =
        Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("kafka-pipeline-drain-", 0).factory());
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(System.Logger.Level.INFO, "Partitions revoked: {0}", partitions);
    metrics.recordRebalance();
    pendingInit.removeAll(partitions);
    dropQueued(partitions);

    if (drainExecutor.isShutdown()) {
      // Pipeline shutdown: consumer.close() revokes the partitions after the workers have stopped
      // and the final commit has run, so there is nothing left to drain or commit.
      clearTracking(partitions);
      return;
    }

    // One budget for the whole revoke: drain listeners and drains share drainTimeout, so the poll
    // thread is never held past it (it must stay below max.poll.interval.ms).
    long deadline = System.nanoTime() + drainTimeout.toNanos();
    notifyDraining(partitions, drainTimeout);
    try {
      Duration left = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
      CompletableFuture<?>[] drains =
          partitions.stream()
              .map(tp -> CompletableFuture.runAsync(() -> drain(tp, left), drainExecutor))
              .toArray(CompletableFuture<?>[]::new);
      CompletableFuture.allOf(drains).join();
      commitSync.run();
    } finally {
      // Always forget the partitions, even if the commit failed, so nothing keeps treating them
      // as draining after they are gone.
      clearTracking(partitions);
      notifyDrained(partitions);
    }
  }

  private void drain(TopicPartition tp, Duration timeout) {
    try {
      PartitionDrainResult result = offsetTracker.drainPartition(tp, timeout);
      if (!result.allCompleted()) {
        metrics.recordDrainTimeout();
        metrics.recordAbandoned(result.abandonedCount());
      }
      logger.log(System.Logger.Level.INFO, "Drained {0}: {1}", tp, result);
    } catch (Exception e) {
      logger.log(
          System.Logger.Level.ERROR, "Error draining partition {0}: {1}", tp, e.getMessage());
    }
  }

  /**
   * Adds a listener told when partitions leave this consumer, such as a sink that must flush what
   * it buffers for them. Not called during shutdown.
   *
   * @param listener the listener
   */
  public void addDrainListener(PartitionDrainListener listener) {
    drainListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  private void notifyDraining(Collection<TopicPartition> partitions, Duration budget) {
    for (PartitionDrainListener listener : drainListeners) {
      try {
        listener.draining(partitions, budget);
      } catch (Throwable e) {
        FatalErrors.rethrowIfJvmFatal(e);
        logger.log(
            System.Logger.Level.WARNING, "Partition drain listener failed for " + partitions, e);
      }
    }
  }

  private void notifyDrained(Collection<TopicPartition> partitions) {
    for (PartitionDrainListener listener : drainListeners) {
      try {
        listener.drained(partitions);
      } catch (Throwable e) {
        FatalErrors.rethrowIfJvmFatal(e);
        logger.log(
            System.Logger.Level.WARNING, "Partition drain listener failed for " + partitions, e);
      }
    }
  }

  private void clearTracking(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      offsetTracker.clearPartition(tp);
    }
  }

  /**
   * Called when this consumer lost its partitions without a normal revoke, typically because the
   * group coordinator kicked it out (missed heartbeats, exceeded {@code max.poll.interval.ms}, or
   * removed by an admin). Other members may already own the partitions, so committing is not
   * possible and draining would only delay rejoining. Queued records are dropped and partition
   * state is cleared; records still running finish, but their acks belong to the old assignment and
   * are discarded. Everything after the last committed offset is redelivered.
   *
   * @param partitions the partitions that were lost
   */
  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(
        System.Logger.Level.WARNING,
        "Partitions lost (consumer was removed from the group): {0}. Uncommitted records will be"
            + " redelivered.",
        partitions);
    metrics.recordRebalance();
    pendingInit.removeAll(partitions);
    dropQueued(partitions);
    boolean notify = !drainExecutor.isShutdown();
    if (notify) notifyDraining(partitions, Duration.ZERO);
    clearTracking(partitions);
    if (notify) notifyDrained(partitions);
  }

  private void dropQueued(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      metrics.partitionRevoked(tp);
      List<? extends ConsumerRecord<?, ?>> abandoned = dispatcher.removePartition(tp);
      if (!abandoned.isEmpty()) {
        counter.completed(abandoned.size(), RecordSize.estimateBytes(abandoned));
        metrics.recordAbandoned(abandoned.size());
      }
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(System.Logger.Level.INFO, "Partitions assigned: {0}", partitions);
    initialize(partitions, INIT_BUDGET);
    for (java.util.function.Consumer<Collection<TopicPartition>> listener : assignListeners) {
      listener.accept(partitions);
    }
  }

  /**
   * Adds a callback run on the poll thread after partitions are assigned and initialized, before
   * the {@code poll()} that assigned them fetches records, such as re-pausing for backpressure.
   *
   * @param listener the callback, given the assigned partitions
   */
  public void addAssignListener(java.util.function.Consumer<Collection<TopicPartition>> listener) {
    assignListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  /**
   * Initializes each partition on its own, so one that fails (for example a {@code position()}
   * timeout) does not leave the others uninitialized. Failed partitions stay paused through {@link
   * #isPendingInit} and are retried by {@link #retryPendingInit()}. The whole pass shares one time
   * budget, and after a timeout the remaining partitions are deferred without blocking further, so
   * the poll thread never stalls long enough to be removed from the group. Fatal errors are
   * rethrown.
   */
  private void initialize(Collection<TopicPartition> partitions, Duration budget) {
    long deadline = System.nanoTime() + budget.toNanos();
    // At most half the budget: the read is optional, position() is not, and a slow coordinator
    // must not leave every partition deferred.
    Map<TopicPartition, OffsetAndMetadata> committed =
        committedOffsets(partitions, budget.dividedBy(2));
    boolean timedOut = false;
    for (TopicPartition tp : partitions) {
      long remainingNanos = deadline - System.nanoTime();
      if (timedOut || remainingNanos <= 0) {
        defer(tp, "initialization time budget used up");
        continue;
      }
      try {
        long position = consumer.position(tp, Duration.ofNanos(remainingNanos));
        // The tracker compares it with the position: it tells whether the group has committed this
        // position yet, and whether the committed metadata (finished offsets) applies here.
        offsetTracker.initPartition(tp, position, committed.get(tp));
        dispatcher.addPartition(tp, offsetTracker.generation(tp));
        metrics.partitionAssigned(tp);
        pendingInit.remove(tp);
        logger.log(System.Logger.Level.DEBUG, "Initialized {0} at offset {1}", tp, position);
      } catch (RuntimeException e) {
        if (FatalErrors.isFatal(e)) throw e;
        if (e instanceof org.apache.kafka.common.errors.TimeoutException) timedOut = true;
        // Undo a half-done init so the retry starts clean.
        dispatcher.removePartition(tp);
        offsetTracker.clearPartition(tp);
        defer(tp, e.toString());
      }
    }
  }

  private void defer(TopicPartition tp, String reason) {
    if (pendingInit.add(tp) || shouldLogRetry()) {
      logger.log(
          System.Logger.Level.WARNING,
          "Could not initialize {0}, keeping it paused and retrying: {1}",
          tp,
          reason);
    }
  }

  /**
   * Retries partitions whose initialization failed. Call from the poll thread before each poll; at
   * most one attempt per second.
   */
  public void retryPendingInit() {
    if (pendingInit.isEmpty()) return;
    long now = System.nanoTime();
    if (now - lastInitRetryNanos < INIT_RETRY_INTERVAL_NANOS) return;
    lastInitRetryNanos = now;
    Set<TopicPartition> retry = new HashSet<>(pendingInit);
    retry.retainAll(consumer.assignment());
    pendingInit.retainAll(retry);
    if (!retry.isEmpty()) initialize(retry, INIT_RETRY_BUDGET);
  }

  /**
   * Whether the partition is assigned but not yet initialized, so it must stay paused.
   *
   * @param tp the partition
   * @return true while initialization is pending
   */
  public boolean isPendingInit(TopicPartition tp) {
    return pendingInit.contains(tp);
  }

  private boolean shouldLogRetry() {
    long now = System.nanoTime();
    if (now - lastInitWarnNanos < INIT_WARN_INTERVAL_NANOS) return false;
    lastInitWarnNanos = now;
    return true;
  }

  private Map<TopicPartition, OffsetAndMetadata> committedOffsets(
      Collection<TopicPartition> partitions, Duration timeout) {
    try {
      return consumer.committed(new HashSet<>(partitions), timeout);
    } catch (Exception e) {
      // Safe to go on without: each partition is then treated as having nothing committed, so its
      // first commit rewrites the position it starts from (redundant at worst), and records
      // finished after the committed offset are processed again.
      logger.log(
          System.Logger.Level.WARNING,
          "Could not read committed offsets for {0}, finished records may be processed again: {1}",
          partitions,
          e.toString());
      return Map.of();
    }
  }

  /**
   * Shuts down the drain executor. Call before closing the consumer: the revoke that {@code
   * close()} triggers then just clears partition state instead of waiting on stopped workers.
   */
  public void shutdown() {
    drainExecutor.shutdownNow();
  }
}
