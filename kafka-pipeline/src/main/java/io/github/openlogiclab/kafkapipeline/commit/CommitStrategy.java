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
package io.github.openlogiclab.kafkapipeline.commit;

/**
 * How the pipeline commits consumed offsets. All methods run on the poll thread, because they use
 * the consumer (or, for a transactional strategy, a producer bound to it).
 *
 * <p>{@link PeriodicCommitter} commits through the consumer: asynchronously on an interval, and
 * synchronously after a drain and at shutdown.
 */
public interface CommitStrategy {

  /** Starts periodic commits. */
  void start();

  /** Called on every poll-loop iteration; commits if the commit interval has elapsed. */
  void maybeCommit();

  /** Commits every partition's current position now, waiting for the result. */
  void commitNow();

  /** Stops periodic commits. */
  void stop();
}
