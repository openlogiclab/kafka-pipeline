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

import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

/**
 * Classifies consumer errors that retrying cannot fix. The pipeline stops on these instead of
 * logging them every poll forever.
 */
public final class FatalErrors {

  private FatalErrors() {}

  /**
   * Returns whether the throwable, or any of its causes, means the consumer cannot make progress
   * without operator action: bad credentials or ACLs, an invalid or fenced group member, a broker
   * that does not support the client, a partition with no offset and {@code
   * auto.offset.reset=none}, or a JVM error (see {@link #isJvmFatal}).
   *
   * @param t the throwable
   * @return true if the pipeline should stop
   */
  public static boolean isFatal(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (isJvmFatal(c)
          || c instanceof AuthenticationException
          || c instanceof AuthorizationException
          || c instanceof InvalidGroupIdException
          || c instanceof FencedInstanceIdException
          || c instanceof UnsupportedVersionException
          || c instanceof NoOffsetForPartitionException) {
        return true;
      }
      if (c.getCause() == c) break;
    }
    return false;
  }

  /**
   * Returns whether the throwable means the JVM itself may be unusable, such as {@link
   * OutOfMemoryError} or {@link InternalError}. {@link StackOverflowError} is excluded: it unwinds
   * the stack of the thread that hit it and is usually caused by one record (deep recursion), so it
   * is treated as an ordinary handler failure rather than stopping the pipeline.
   *
   * @param t the throwable
   * @return true if the pipeline must stop instead of skipping the record
   */
  public static boolean isJvmFatal(Throwable t) {
    return t instanceof VirtualMachineError && !(t instanceof StackOverflowError);
  }

  /**
   * Rethrows the throwable if {@link #isJvmFatal} says so.
   *
   * @param t the throwable
   */
  public static void rethrowIfJvmFatal(Throwable t) {
    if (isJvmFatal(t)) throw (Error) t;
  }
}
