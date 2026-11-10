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

import io.github.openlogiclab.kafkapipeline.backpressure.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import io.github.openlogiclab.kafkapipeline.internal.PipelineMetricsCollector;
import io.github.openlogiclab.kafkapipeline.internal.RecordSize;
import io.github.openlogiclab.kafkapipeline.offset.PartitionDrainResult;
import io.github.openlogiclab.kafkapipeline.offset.PartitionLifecycle;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
  private final PipelineMetricsCollector metricsCollector;
  private final ExecutorService drainExecutor;
  private final boolean readCommittedMetadata;

  private static final long INIT_RETRY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static final long INIT_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
  // Total time one initialization pass may block the poll thread, kept well below the default
  // max.poll.interval.ms (5 minutes) however many partitions are assigned.
  private static final Duration INIT_BUDGET = Duration.ofSeconds(10);
  private static final Duration INIT_RETRY_BUDGET = Duration.ofSeconds(5);
  // Assigned partitions whose initialization failed; poll thread only.
  private final Set<TopicPartition> pendingInit = new HashSet<>();
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
   * @param metricsCollector the metrics collector
   */
  public PipelineRebalanceListener(
      PartitionLifecycle offsetTracker,
      RecordDispatcher<?, ?> dispatcher,
      Runnable commitSync,
      InFlightCounter counter,
      Consumer<?, ?> consumer,
      Duration drainTimeout,
      PipelineMetricsCollector metricsCollector) {
    this(
        offsetTracker,
        dispatcher,
        commitSync,
        counter,
        consumer,
        drainTimeout,
        metricsCollector,
        false);
  }

  /**
   * Creates a rebalance listener.
   *
   * @param offsetTracker the offset tracker
   * @param dispatcher the record dispatcher
   * @param commitSync callback to perform sync commit
   * @param counter the in-flight counter
   * @param consumer the Kafka consumer
   * @param drainTimeout timeout for draining in-flight records
   * @param metricsCollector the metrics collector
   * @param readCommittedMetadata whether to read the committed metadata of newly assigned
   *     partitions, so records the previous owner already finished are skipped
   */
  public PipelineRebalanceListener(
      PartitionLifecycle offsetTracker,
      RecordDispatcher<?, ?> dispatcher,
      Runnable commitSync,
      InFlightCounter counter,
      Consumer<?, ?> consumer,
      Duration drainTimeout,
      PipelineMetricsCollector metricsCollector,
      boolean readCommittedMetadata) {
    this.readCommittedMetadata = readCommittedMetadata;
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.commitSync = commitSync;
    this.counter = counter;
    this.consumer = consumer;
    this.drainTimeout = drainTimeout;
    this.metricsCollector = metricsCollector;
    this.drainExecutor =
        Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("kafka-pipeline-drain-", 0).factory());
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(System.Logger.Level.INFO, "Partitions revoked: {0}", partitions);
    metricsCollector.recordRebalance();
    pendingInit.removeAll(partitions);
    dropQueued(partitions);

    if (drainExecutor.isShutdown()) {
      // Pipeline shutdown: consumer.close() revokes the partitions after the workers have stopped
      // and the final commit has run, so there is nothing left to drain or commit.
      clearTracking(partitions);
      return;
    }

    CompletableFuture<?>[] drains =
        partitions.stream()
            .map(tp -> CompletableFuture.runAsync(() -> drain(tp), drainExecutor))
            .toArray(CompletableFuture<?>[]::new);
    CompletableFuture.allOf(drains).join();

    commitSync.run();
    clearTracking(partitions);
  }

  private void drain(TopicPartition tp) {
    try {
      PartitionDrainResult result = offsetTracker.drainPartition(tp, drainTimeout);
      if (!result.allCompleted()) {
        metricsCollector.recordDrainTimeout();
        metricsCollector.recordAbandoned(result.abandonedCount());
      }
      logger.log(System.Logger.Level.INFO, "Drained {0}: {1}", tp, result);
    } catch (Exception e) {
      logger.log(
          System.Logger.Level.ERROR, "Error draining partition {0}: {1}", tp, e.getMessage());
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
    metricsCollector.recordRebalance();
    pendingInit.removeAll(partitions);
    dropQueued(partitions);
    clearTracking(partitions);
  }

  private void dropQueued(Collection<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      metricsCollector.partitionRevoked(tp);
      List<? extends ConsumerRecord<?, ?>> abandoned = dispatcher.removePartition(tp);
      if (!abandoned.isEmpty()) {
        counter.completed(abandoned.size(), RecordSize.estimateBytes(abandoned));
        metricsCollector.recordAbandoned(abandoned.size());
      }
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(System.Logger.Level.INFO, "Partitions assigned: {0}", partitions);
    initialize(partitions, INIT_BUDGET);
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
    Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(partitions, budget);
    boolean timedOut = false;
    for (TopicPartition tp : partitions) {
      long remainingNanos = deadline - System.nanoTime();
      if (timedOut || remainingNanos <= 0) {
        defer(tp, "initialization time budget used up");
        continue;
      }
      try {
        long position = consumer.position(tp, Duration.ofNanos(remainingNanos));
        OffsetAndMetadata last = committed.get(tp);
        // The metadata only describes records after the committed offset, so it applies only when
        // this consumer resumes exactly there (not after an offset reset or a seek).
        String metadata = last != null && last.offset() == position ? last.metadata() : null;
        offsetTracker.initPartition(tp, position, metadata);
        dispatcher.addPartition(tp, offsetTracker.generation(tp));
        metricsCollector.partitionAssigned(tp);
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
    if (!readCommittedMetadata) {
      return Map.of();
    }
    try {
      return consumer.committed(new HashSet<>(partitions), timeout);
    } catch (Exception e) {
      // Only an optimisation: without it, records finished after the committed offset are
      // processed again, exactly as they would be without completed-offset tracking.
      logger.log(
          System.Logger.Level.WARNING,
          "Could not read committed metadata for {0}, finished records may be processed again: {1}",
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
