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

import io.github.openlogiclab.kafkapipeline.error.WriteFailure;
import java.util.function.Function;

/**
 * Runs a user's {@code classify} (of a sink or a DLQ handler) so that a broken one cannot stall or
 * stop the pipeline. Every write failure is classified through here.
 */
public final class Classification {

  private Classification() {}

  /**
   * Classifies a failure. A JVM error is always {@link WriteFailure#FATAL}. A classifier that
   * returns null, or throws anything but a JVM error, gives {@link WriteFailure#RECORD}: retried up
   * to the error strategy's budget, then dead-lettered or dropped, so the record never hangs and
   * the pipeline does not stop because of a classifier bug.
   *
   * @param classifier the user's classifier
   * @param error the failure to classify
   * @param <E> the failure type the classifier takes
   * @return the classification, never null
   */
  public static <E extends Throwable> WriteFailure classify(
      Function<? super E, WriteFailure> classifier, E error) {
    if (FatalErrors.isJvmFatal(error)) return WriteFailure.FATAL;
    try {
      WriteFailure kind = classifier.apply(error);
      return kind != null ? kind : WriteFailure.RECORD;
    } catch (Throwable t) {
      FatalErrors.rethrowIfJvmFatal(t);
      return WriteFailure.RECORD;
    }
  }
}
