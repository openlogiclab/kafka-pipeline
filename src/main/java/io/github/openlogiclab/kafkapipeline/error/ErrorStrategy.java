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
package io.github.openlogiclab.kafkapipeline.error;

import java.time.Duration;

/**
 * Defines how the pipeline handles processing failures.
 *
 * <p>Execution order on failure:
 *
 * <ol>
 *   <li>Retry up to {@code maxRetries} times with {@code retryBackoff} delay (exponential if {@code
 *       exponentialBackoff} is true, capped at {@code maxBackoff})
 *   <li>If retries exhausted and {@code dlqHandler} is set, attempt DLQ send
 *   <li>If DLQ succeeds → ack the record (offset advances)
 *   <li>If DLQ fails or is not configured → apply {@code fallback} (SKIP or FAIL_PARTITION)
 * </ol>
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public record ErrorStrategy<K, V>(
    int maxRetries,
    Duration retryBackoff,
    boolean exponentialBackoff,
    Duration maxBackoff,
    DLQHandler<K, V> dlqHandler,
    Fallback fallback) {
  public ErrorStrategy {
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
    }
    if (retryBackoff.isNegative()) {
      throw new IllegalArgumentException("retryBackoff must not be negative");
    }
    if (maxBackoff.isNegative()) {
      throw new IllegalArgumentException("maxBackoff must not be negative");
    }
  }

  /** No retries, no DLQ, fail the partition on any error. */
  public static <K, V> ErrorStrategy<K, V> failFast() {
    return new ErrorStrategy<>(
        0, Duration.ZERO, false, Duration.ZERO, null, Fallback.FAIL_PARTITION);
  }

  /** No retries, no DLQ, skip failed records. */
  public static <K, V> ErrorStrategy<K, V> skipOnError() {
    return new ErrorStrategy<>(0, Duration.ZERO, false, Duration.ZERO, null, Fallback.SKIP);
  }

  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /** Compute the backoff duration for a given attempt number (0-based). */
  public Duration backoffForAttempt(int attempt) {
    if (attempt <= 0 || retryBackoff.isZero()) {
      return retryBackoff;
    }
    if (!exponentialBackoff) {
      return retryBackoff;
    }
    long millis = retryBackoff.toMillis() * (1L << Math.min(attempt, 30));
    long capped = Math.min(millis, maxBackoff.isZero() ? millis : maxBackoff.toMillis());
    return Duration.ofMillis(capped);
  }

  public boolean hasDlq() {
    return dlqHandler != null;
  }

  /** Builder for {@link ErrorStrategy}. */
  public static final class Builder<K, V> {
    private int maxRetries = 0;
    private Duration retryBackoff = Duration.ofSeconds(1);
    private boolean exponentialBackoff = false;
    private Duration maxBackoff = Duration.ofMinutes(1);
    private DLQHandler<K, V> dlqHandler;
    private Fallback fallback = Fallback.FAIL_PARTITION;

    private Builder() {}

    /**
     * Maximum number of retry attempts before moving to DLQ/fallback. Set to {@code 0} to disable
     * retries.
     *
     * <p>Default: {@code 0} (no retries).
     */
    public Builder<K, V> maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Base delay between retry attempts. When {@link #exponentialBackoff} is enabled, this is
     * multiplied by powers of 2 on each subsequent attempt.
     *
     * <p>Default: {@code 1 second}.
     */
    public Builder<K, V> retryBackoff(Duration retryBackoff) {
      this.retryBackoff = retryBackoff;
      return this;
    }

    /**
     * Whether to apply exponential backoff (base × 2^attempt) instead of fixed delay.
     *
     * <p>Default: {@code false} (fixed delay).
     */
    public Builder<K, V> exponentialBackoff(boolean exponentialBackoff) {
      this.exponentialBackoff = exponentialBackoff;
      return this;
    }

    /**
     * Upper bound for exponential backoff. Ignored when exponential backoff is disabled.
     *
     * <p>Default: {@code 1 minute}.
     */
    public Builder<K, V> maxBackoff(Duration maxBackoff) {
      this.maxBackoff = maxBackoff;
      return this;
    }

    /**
     * Optional dead letter queue handler. When set, failed records (after all retries) are sent to
     * the DLQ before applying the fallback. If the DLQ send succeeds, the record is acked and the
     * offset advances.
     *
     * <p>Default: {@code null} (no DLQ).
     */
    public Builder<K, V> dlqHandler(DLQHandler<K, V> dlqHandler) {
      this.dlqHandler = dlqHandler;
      return this;
    }

    /**
     * What to do when all retries fail and DLQ is unavailable or also fails.
     *
     * <ul>
     *   <li>{@link Fallback#SKIP} — ack the record and move on (data loss for this record)
     *   <li>{@link Fallback#FAIL_PARTITION} — halt processing for the affected partition
     * </ul>
     *
     * <p>Default: {@link Fallback#FAIL_PARTITION}.
     */
    public Builder<K, V> fallback(Fallback fallback) {
      this.fallback = fallback;
      return this;
    }

    public ErrorStrategy<K, V> build() {
      return new ErrorStrategy<>(
          maxRetries, retryBackoff, exponentialBackoff, maxBackoff, dlqHandler, fallback);
    }
  }
}
