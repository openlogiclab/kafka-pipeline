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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.github.openlogiclab.kafkapipeline.InFlightCounter;
import io.github.openlogiclab.kafkapipeline.RecordSize;
import io.github.openlogiclab.kafkapipeline.ThreadMode;
import io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingContext;
import io.github.openlogiclab.kafkapipeline.handler.ProcessingLifecycleHook;
import io.github.openlogiclab.kafkapipeline.handler.RecordHandler;
import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Per-record processing mode. Worker threads pull from {@link RecordDispatcher} queues and execute:
 *
 * <pre>
 *   markInProgress → hook.beforeProcess
 *       → handler.handle (with retry)
 *       → success: hook.afterProcess → ack
 *       → failure: DLQ → skip / fail partition
 * </pre>
 */
public final class SingleRecordWorkerPool<K, V> extends WorkerPool<K, V> {

  private static final System.Logger logger =
      System.getLogger(SingleRecordWorkerPool.class.getName());

  private final RecordHandler<K, V> handler;
  private final ProcessingLifecycleHook<K, V> hook;
  private final RetryExecutor<K, V> retryExecutor;
  private final OffsetTracker offsetTracker;
  private final RecordDispatcher<K, V> dispatcher;
  private final InFlightCounter counter;

  public SingleRecordWorkerPool(
      int concurrency,
      ThreadMode threadMode,
      RecordHandler<K, V> handler,
      ProcessingLifecycleHook<K, V> hook,
      RetryExecutor<K, V> retryExecutor,
      OffsetTracker offsetTracker,
      RecordDispatcher<K, V> dispatcher,
      InFlightCounter counter) {
    super(concurrency, threadMode, "kafka-pipeline-worker-", 0);
    this.handler = handler;
    this.hook = hook;
    this.retryExecutor = retryExecutor;
    this.offsetTracker = offsetTracker;
    this.dispatcher = dispatcher;
    this.counter = counter;
  }

  @Override
  public void start() {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("WorkerPool already started");
    }
    for (int i = 0; i < concurrency; i++) {
      executor.submit(this::workerLoop);
    }
    logger.log(
        System.Logger.Level.INFO,
        "SingleRecordWorkerPool started with {0} workers ({1} threads)",
        concurrency,
        threadMode);
  }

  @Override
  public void dispatch(ConsumerRecords<K, V> records) {
    for (ConsumerRecord<K, V> record : records) {
      TopicPartition tp = new TopicPartition(record.topic(), record.partition());
      offsetTracker.register(tp, record.offset());
      counter.registered(1, RecordSize.estimateBytes(record));
      dispatcher.dispatch(tp, record);
    }
    dispatcher.wakeWorkers();
  }

  private void workerLoop() {
    while (running.get()) {
      try {
        if (Thread.currentThread().isInterrupted()) break;
        RecordDispatcher.PollResult<K, V> result = dispatcher.poll(100, MILLISECONDS);
        if (result == null) continue;
        processRecord(result.partition(), result.record());
      } catch (Exception e) {
        logger.log(System.Logger.Level.ERROR, "Unexpected error in worker loop", e);
      }
    }
  }

  private void processRecord(TopicPartition tp, ConsumerRecord<K, V> record) {
    long offset = record.offset();
    long recordBytes = RecordSize.estimateBytes(record);
    ProcessingContext ctx = new ProcessingContext(tp, offset, 0);

    try {
      offsetTracker.markInProgress(tp, offset);
    } catch (IllegalStateException e) {
      logger.log(
          System.Logger.Level.WARNING,
          "Cannot mark offset {0} in-progress for {1}: {2}",
          offset,
          tp,
          e.getMessage());
      counter.completed(1, recordBytes);
      return;
    }

    boolean shouldSkipViaHook = false;
    try {
      shouldSkipViaHook = !hook.beforeProcess(record, ctx);
    } catch (Exception e) {
      logger.log(
          System.Logger.Level.WARNING,
          "beforeProcess hook threw for {0}:{1}, proceeding with handler",
          tp,
          offset);
    }

    if (shouldSkipViaHook) {
      offsetTracker.ack(tp, offset);
      counter.completed(1, recordBytes);
      return;
    }

    String desc = "offset " + offset;
    Exception lastError =
        retryExecutor.executeWithRetries(
            attempt -> {
              ProcessingContext attemptCtx = new ProcessingContext(tp, offset, attempt);
              try {
                handler.handle(record);
                hook.afterProcess(record, attemptCtx);
              } catch (Exception ex) {
                hook.onError(record, attemptCtx, ex);
                throw ex;
              }
            },
            tp,
            desc);

    if (lastError == null) {
      offsetTracker.ack(tp, offset);
      counter.completed(1, recordBytes);
      return;
    }

    RetryExecutor.FailureResolution resolution =
        retryExecutor.handleFailure(List.of(record), tp, lastError, desc);

    switch (resolution) {
      case DLQ_SUCCESS, SKIP -> offsetTracker.ack(tp, offset);
      case FAIL_PARTITION -> offsetTracker.fail(tp, offset);
    }
    counter.completed(1, recordBytes);
  }
}
