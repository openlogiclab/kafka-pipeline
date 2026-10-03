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
 * Error handling strategy after all retries are exhausted.
 *
 * <p>This is a best-effort strategy: the system always continues processing. Failed records are
 * always skipped after invoking the {@link FinalFailureHandler}.
 */
public enum Fallback {

  /**
   * Direct skip — do not attempt DLQ, skip the record immediately. The {@link FinalFailureHandler}
   * is called before skipping for logging/alerting.
   */
  SKIP,

  /**
   * Try DLQ first, then skip if DLQ fails. If a {@link DLQHandler} is configured, the record is
   * sent there. If DLQ succeeds, the record is acked normally. If DLQ fails (or is not configured),
   * the {@link FinalFailureHandler} is called and the record is skipped.
   */
  DLQ_THEN_SKIP
}
