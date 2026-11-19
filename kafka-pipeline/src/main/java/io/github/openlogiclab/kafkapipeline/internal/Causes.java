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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Walks a throwable's cause chain safely: a chain may loop back on itself (A caused by B caused by
 * A).
 */
public final class Causes {

  /**
   * Most causes looked at. Real chains are a few levels deep; this only bounds a pathological
   * chain, which the identity check alone would let grow as long as memory allows.
   */
  public static final int MAX_DEPTH = 100;

  private Causes() {}

  /**
   * Returns whether the throwable or one of its causes matches. Each throwable is tested at most
   * once, so a cause cycle ends the walk instead of looping forever.
   *
   * @param t the throwable, may be null
   * @param test the test
   * @return true if one of them matches
   */
  public static boolean anyMatch(Throwable t, Predicate<Throwable> test) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    int depth = 0;
    for (Throwable c = t; c != null && depth < MAX_DEPTH && seen.add(c); c = c.getCause()) {
      if (test.test(c)) return true;
      depth++;
    }
    return false;
  }
}
