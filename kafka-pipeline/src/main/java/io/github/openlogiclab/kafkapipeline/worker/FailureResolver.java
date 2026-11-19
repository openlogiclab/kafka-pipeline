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

import io.github.openlogiclab.kafkapipeline.offset.ProcessingProgress;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Decides what happens to records whose handler still failed after every retry. Shared by both
 * worker pools so the rule lives in one place: on shutdown leave them unacked for redelivery;
 * otherwise send them to the DLQ or drop them, and ack either way so the partition moves on.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
final class FailureResolver<K, V> {

  private static final System.Logger logger = System.getLogger(FailureResolver.class.getName());

  private final RetryExecutor<K, V> retryExecutor;
  private final ProcessingProgress progress;
  private final BooleanSupplier stopping;
  private final Consumer<Throwable> onFatal;

  FailureResolver(
      RetryExecutor<K, V> retryExecutor,
      ProcessingProgress progress,
      BooleanSupplier stopping,
      Consumer<Throwable> onFatal) {
    this.retryExecutor = retryExecutor;
    this.progress = progress;
    this.stopping = stopping;
    this.onFatal = onFatal;
  }

  /**
   * Resolves records whose processing (or sink write) failed for good: sends them to the DLQ or
   * drops them, and acks them either way. Records are left unacked, for redelivery, when the
   * pipeline is stopping, the partition moved to another assignment, or the DLQ failed fatally.
   *
   * @param records the records that failed
   * @param tp their partition
   * @param error the last error
   * @param desc a description for logs, such as {@code "offset 5"}
   * @param ack acks the records; returns false if the partition is no longer tracked
   * @param owned whether this consumer still owns the partition in the records' assignment
   */
  void resolve(
      List<ConsumerRecord<K, V>> records,
      TopicPartition tp,
      Exception error,
      String desc,
      BooleanSupplier ack,
      BooleanSupplier owned) {
    if (Thread.currentThread().isInterrupted()) {
      // Interrupted by shutdown, not a processing failure: leave the records unacked so they are
      // not committed and are redelivered.
      logger.log(
          System.Logger.Level.DEBUG, "Interrupted while processing {0} {1}, not acking", tp, desc);
      return;
    }
    if (!owned.getAsBoolean()) {
      // The partition moved to another assignment: its records belong to the new owner, which
      // processes them again. Neither dead-letter nor drop them here.
      logger.log(
          System.Logger.Level.DEBUG,
          "{0} {1} failed after its partition moved on, not acking",
          tp,
          desc);
      return;
    }
    RetryExecutor.FailureResolution resolution =
        retryExecutor.handleFailure(records, tp, error, desc, stopping, owned, onFatal);
    switch (resolution) {
      case ABANDONED -> {
        // Neither dead-lettered nor dropped, so not acked: redelivered.
      }
      case DROPPED -> {
        if (ack.getAsBoolean()) progress.markDropped(tp);
      }
      case DLQ_SUCCESS -> ack.getAsBoolean();
    }
  }
}
