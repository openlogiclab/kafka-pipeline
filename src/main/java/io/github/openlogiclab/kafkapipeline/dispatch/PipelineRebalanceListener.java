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

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.RecordSize;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import io.github.openlogiclab.kafkapipeline.offset.PartitionDrainResult;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

  private final OffsetTracker offsetTracker;
  private final RecordDispatcher<?, ?> dispatcher;
  private final Runnable commitSync;
  private final InFlightCounter counter;
  private final Consumer<?, ?> consumer;
  private final Duration drainTimeout;

  public PipelineRebalanceListener(
      OffsetTracker offsetTracker,
      RecordDispatcher<?, ?> dispatcher,
      Runnable commitSync,
      InFlightCounter counter,
      Consumer<?, ?> consumer,
      Duration drainTimeout) {
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.commitSync = commitSync;
    this.counter = counter;
    this.consumer = consumer;
    this.drainTimeout = drainTimeout;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(System.Logger.Level.INFO, "Partitions revoked: {0}", partitions);

    for (TopicPartition tp : partitions) {
      List<? extends ConsumerRecord<?, ?>> abandoned = dispatcher.removePartition(tp);
      if (!abandoned.isEmpty()) {
        long abandonedBytes = 0;
        for (ConsumerRecord<?, ?> record : abandoned) {
          abandonedBytes += RecordSize.estimateBytes(record);
        }
        counter.completed(abandoned.size(), abandonedBytes);
      }
    }

    CompletableFuture<?>[] drainFutures =
        partitions.stream()
            .map(
                tp ->
                    CompletableFuture.runAsync(
                        () -> {
                          PartitionDrainResult result =
                              offsetTracker.drainPartition(tp, drainTimeout);
                          logger.log(System.Logger.Level.INFO, "Drained {0}: {1}", tp, result);
                        }))
            .toArray(CompletableFuture<?>[]::new);

    CompletableFuture.allOf(drainFutures).join();

    commitSync.run();

    for (TopicPartition tp : partitions) {
      offsetTracker.clearPartition(tp);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) return;

    logger.log(System.Logger.Level.INFO, "Partitions assigned: {0}", partitions);

    for (TopicPartition tp : partitions) {
      long position = consumer.position(tp);
      offsetTracker.initPartition(tp, position);
      dispatcher.addPartition(tp);

      logger.log(System.Logger.Level.DEBUG, "Initialized {0} at offset {1}", tp, position);
    }
  }
}
