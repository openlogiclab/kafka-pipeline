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

/**
 * What happens to a record whose retries are used up.
 *
 * <p>The pipeline never stops for a bad record: a record is either dead-lettered or dropped, and
 * acked either way, so its partition keeps moving. "Dropped" means given up on: the {@link
 * FinalFailureHandler} is called with the record and the error, the drop is counted in the metrics,
 * and the record is not processed again.
 */
public enum Fallback {

  /**
   * Drop the record without trying the DLQ. The {@link FinalFailureHandler} is called first, for
   * logging or alerting.
   */
  DROP,

  /**
   * Send the record to the {@link DLQHandler}; if that fails, or no DLQ is configured, drop it
   * (calling the {@link FinalFailureHandler} first). The default.
   */
  DLQ_THEN_DROP
}
