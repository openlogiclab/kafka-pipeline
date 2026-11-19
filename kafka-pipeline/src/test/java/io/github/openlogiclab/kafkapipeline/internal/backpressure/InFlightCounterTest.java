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
package io.github.openlogiclab.kafkapipeline.internal.backpressure;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class InFlightCounterTest {

  @Test
  void aCompletionSeenBeforeItsRegistration_neverReadsNegative() {
    // LongAdder.sum() is not atomic across cells, so a reader can see a worker's completion before
    // the poll thread's registration. The same skew, made deterministic:
    InFlightCounter counter = new InFlightCounter();
    counter.completed(3, 300);
    assertEquals(0, counter.records());
    assertEquals(0, counter.bytes());

    counter.registered(3, 300);
    counter.registered(2, 50);
    assertEquals(2, counter.records());
    assertEquals(50, counter.bytes());
  }
}
