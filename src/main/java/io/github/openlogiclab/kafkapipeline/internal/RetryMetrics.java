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
package io.github.openlogiclab.kafkapipeline.internal;

/**
 * Metrics the retry executor records: retries, timeouts, and how failed records end. Handler
 * attempts are reported to {@link HandlerAttemptObserver}.
 *
 * <p><strong>This is an internal API, not intended for direct use by library consumers.</strong>
 */
public interface RetryMetrics {

  /** Records a retry of a failed attempt. */
  void recordRetry();

  /** Records a handler attempt interrupted by the processing timeout. */
  void recordProcessingTimeout();

  /**
   * Records records sent to the DLQ.
   *
   * @param count number of records sent
   */
  void recordDlqSuccess(int count);

  /**
   * Records records whose DLQ send failed.
   *
   * @param count number of records that failed to send
   */
  void recordDlqFailure(int count);

  /**
   * Records dropped records: all retries failed and the records did not reach the DLQ. Do not count
   * records that were sent to the DLQ; those go to {@link #recordDlqSuccess}.
   *
   * @param count number of records dropped
   */
  void recordFailed(int count);

  /** Records a final failure handler invocation, one per dropped record. */
  void recordFinalFailure();
}
