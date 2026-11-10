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

import io.github.openlogiclab.kafkapipeline.offset.OffsetStats;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
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
  private final OffsetStats offsetTracker;
  private final int pauseAt;
  private final int resumeAt;
  private final Predicate<TopicPartition> hasRoom;
  private final Set<TopicPartition> windowPaused = new HashSet<>();
  // Partitions whose records were sent back with a seek because they did not fit (full window or
  // full queue). They resume once the window has drained and the worker pool has room again.
  private final Set<TopicPartition> rejectedPaused = new HashSet<>();
  // Other per-partition reasons to stay paused (partition not initialized yet, ordered batch still
  // running). Each is a limit of that one partition, never a substitute for global backpressure.
  private final List<Predicate<TopicPartition>> holds = new ArrayList<>();

  /**
   * Adds a per-partition reason to keep a partition paused, checked before every poll. The
   * partition resumes only when no hold, no window or rejection pause, and no global throttle
   * applies.
   *
   * @param hold returns true while the partition must not be fetched
   */
  public void addHold(Predicate<TopicPartition> hold) {
    holds.add(hold);
  }

  private boolean held(TopicPartition tp) {
    for (Predicate<TopicPartition> hold : holds) {
      if (hold.test(tp)) return true;
    }
    return false;
  }

  /**
   * Creates a pause controller.
   *
   * @param consumer the Kafka consumer whose partitions are paused and resumed
   * @param offsetTracker source of per-partition window sizes
   */
  public PartitionPauseController(Consumer<?, ?> consumer, OffsetStats offsetTracker) {
    this(consumer, offsetTracker, tp -> true);
  }

  /**
   * Creates a pause controller.
   *
   * @param consumer the Kafka consumer whose partitions are paused and resumed
   * @param offsetTracker source of per-partition window sizes
   * @param hasRoom whether the worker pool can take a partition's records again after they were
   *     sent back because its queue was full
   */
  public PartitionPauseController(
      Consumer<?, ?> consumer, OffsetStats offsetTracker, Predicate<TopicPartition> hasRoom) {
    this.hasRoom = hasRoom;
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
    rejectedPaused.retainAll(assignment);

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
      if (rejectedPaused.contains(tp) && size <= resumeAt && hasRoom.test(tp)) {
        rejectedPaused.remove(tp);
      }
    }

    Set<TopicPartition> paused = consumer.paused();
    List<TopicPartition> toPause = new ArrayList<>();
    List<TopicPartition> toResume = new ArrayList<>();
    for (TopicPartition tp : assignment) {
      boolean shouldPause =
          throttled || windowPaused.contains(tp) || rejectedPaused.contains(tp) || held(tp);
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
   * Handles records that did not fit (full offset window or full worker queue): seeks each
   * partition back to the first record that was not dispatched, so it is fetched again, and pauses
   * the partition until there is room.
   *
   * @param rejected first undispatched offset per partition
   */
  public void seekBack(Map<TopicPartition, Long> rejected) {
    if (rejected.isEmpty()) return;
    for (Map.Entry<TopicPartition, Long> entry : rejected.entrySet()) {
      TopicPartition tp = entry.getKey();
      long offset = entry.getValue();
      consumer.seek(tp, offset);
      rejectedPaused.add(tp);
      logger.log(
          System.Logger.Level.DEBUG,
          "No room for {0} from offset {1}; seeking back and pausing until there is room",
          tp,
          offset);
    }
    consumer.pause(rejected.keySet());
  }

  /**
   * Returns whether a partition is paused by this controller for any reason other than global
   * throttling.
   *
   * @param tp the partition
   * @return true if window-paused or paused after its records were sent back
   */
  public boolean isPaused(TopicPartition tp) {
    return windowPaused.contains(tp) || rejectedPaused.contains(tp) || held(tp);
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
