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

import io.github.openlogiclab.kafkapipeline.offset.OffsetTracker;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Decides which assigned partitions the consumer fetches from. A partition is paused when either
 * global backpressure is throttling, or its own offset window is nearly full (typically because a
 * slow record at its head keeps completed records from being committed). Window pauses use
 * hysteresis: pause at {@value #PAUSE_RATIO_PERCENT}% of the window, resume at {@value
 * #RESUME_RATIO_PERCENT}%, so other partitions keep flowing while one is stuck.
 *
 * <p>The desired pause state is reapplied before every poll from {@link Consumer#paused()}, so
 * partitions assigned by a rebalance pick up the current state before they are fetched.
 *
 * <p>Not thread-safe: call only from the poll thread.
 */
public final class PartitionPauseController {

  private static final System.Logger logger =
      System.getLogger(PartitionPauseController.class.getName());

  static final int PAUSE_RATIO_PERCENT = 80;
  static final int RESUME_RATIO_PERCENT = 50;

  private final Consumer<?, ?> consumer;
  private final OffsetTracker offsetTracker;
  private final int pauseAt;
  private final int resumeAt;
  private final Set<TopicPartition> windowPaused = new HashSet<>();

  /**
   * Creates a pause controller.
   *
   * @param consumer the Kafka consumer whose partitions are paused and resumed
   * @param offsetTracker source of per-partition window sizes
   */
  public PartitionPauseController(Consumer<?, ?> consumer, OffsetTracker offsetTracker) {
    this.consumer = consumer;
    this.offsetTracker = offsetTracker;
    int max = offsetTracker.maxWindowSize();
    this.pauseAt = Math.max(1, (int) ((long) max * PAUSE_RATIO_PERCENT / 100));
    this.resumeAt = (int) ((long) max * RESUME_RATIO_PERCENT / 100);
  }

  /**
   * Pauses and resumes partitions to match the current state. Call before every poll.
   *
   * @param throttled whether global backpressure is throttling all partitions
   */
  public void apply(boolean throttled) {
    Set<TopicPartition> assignment = consumer.assignment();
    windowPaused.retainAll(assignment);

    for (TopicPartition tp : assignment) {
      int size = offsetTracker.windowSize(tp);
      if (windowPaused.contains(tp)) {
        if (size <= resumeAt) {
          windowPaused.remove(tp);
          logger.log(
              System.Logger.Level.INFO,
              "Window has room again, resuming {0} (window={1})",
              tp,
              size);
        }
      } else if (size >= pauseAt) {
        windowPaused.add(tp);
        logger.log(
            System.Logger.Level.INFO,
            "Window nearly full, pausing {0} (window={1}, max={2}). "
                + "A slow record is likely holding back commits on this partition.",
            tp,
            size,
            offsetTracker.maxWindowSize());
      }
    }

    Set<TopicPartition> paused = consumer.paused();
    List<TopicPartition> toPause = new ArrayList<>();
    List<TopicPartition> toResume = new ArrayList<>();
    for (TopicPartition tp : assignment) {
      boolean shouldPause = throttled || windowPaused.contains(tp);
      boolean isPaused = paused.contains(tp);
      if (shouldPause && !isPaused) {
        toPause.add(tp);
      } else if (!shouldPause && isPaused) {
        toResume.add(tp);
      }
    }
    if (!toPause.isEmpty()) consumer.pause(toPause);
    if (!toResume.isEmpty()) consumer.resume(toResume);
  }

  /**
   * Handles records that did not fit in their partition's window: seeks each partition back to the
   * first record that was not dispatched, so it is fetched again, and pauses the partition until
   * its window has room.
   *
   * @param rejected first undispatched offset per partition
   */
  public void seekBack(Map<TopicPartition, Long> rejected) {
    if (rejected.isEmpty()) return;
    for (Map.Entry<TopicPartition, Long> entry : rejected.entrySet()) {
      TopicPartition tp = entry.getKey();
      long offset = entry.getValue();
      consumer.seek(tp, offset);
      windowPaused.add(tp);
      logger.log(
          System.Logger.Level.WARNING,
          "Window full for {0}, seeking back to offset {1} and pausing until it drains",
          tp,
          offset);
    }
    consumer.pause(rejected.keySet());
  }

  /**
   * Returns whether a partition is currently paused because its window is nearly full.
   *
   * @param tp the partition
   * @return true if window-paused
   */
  public boolean isWindowPaused(TopicPartition tp) {
    return windowPaused.contains(tp);
  }
}
