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

import io.github.openlogiclab.kafkapipeline.internal.FatalErrors;
import org.apache.kafka.common.errors.RetriableException;

/**
 * What a failed write (a sink write or a DLQ send) says about the destination, which decides what
 * the pipeline does next. Sinks and DLQ handlers classify their own failures ({@code
 * RecordSink.classify}, {@link DLQHandler#classify}); {@link #of(Throwable)} is the default for
 * Kafka clients.
 */
public enum WriteFailure {

  /**
   * Something is wrong with this record's write (too large, invalid, rejected): retry it up to the
   * error strategy's {@code maxRetries}, then send it to the DLQ or drop it.
   */
  RECORD,

  /**
   * The destination is unavailable for now (a timeout, a leader election, a failover). Retry until
   * it recovers, without dropping the record: the record keeps its slot, so an outage turns into
   * backpressure. If the destination stays unavailable longer than the pipeline's {@code
   * writeOutageTimeout}, the pipeline stops instead, still without dropping the record.
   */
  TRANSIENT,

  /**
   * Every write would fail the same way (bad credentials, a missing ACL, an unsupported version, a
   * closed client): stop the pipeline and leave the record unacked. Dropping it would commit input
   * whose output was never written.
   */
  FATAL;

  /**
   * Classifies a Kafka client failure: authentication, authorization, unsupported-version and
   * similar errors are {@link #FATAL}; {@link RetriableException}s (including producer timeouts)
   * are {@link #TRANSIENT}; everything else is {@link #RECORD}.
   *
   * @param error the failure
   * @return its classification
   */
  public static WriteFailure of(Throwable error) {
    if (FatalErrors.isFatal(error)) return FATAL;
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof RetriableException) return TRANSIENT;
      if (t.getCause() == t) break;
    }
    return RECORD;
  }
}
