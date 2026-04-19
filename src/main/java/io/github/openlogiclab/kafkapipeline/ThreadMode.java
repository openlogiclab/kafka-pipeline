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
package io.github.openlogiclab.kafkapipeline;

/**
 * Controls which thread type the worker pool uses.
 *
 * <p>
 *
 * <ul>
 *   <li>{@link #PLATFORM} — OS-level threads (default). Fixed pool of {@code concurrency} threads.
 *       Best for CPU-bound handlers or when you need predictable thread count.
 *   <li>{@link #VIRTUAL} — JDK 21+ virtual threads. One virtual thread per record, managed by the
 *       JVM scheduler. Best for I/O-bound handlers (DB calls, HTTP, etc.) where thousands of
 *       concurrent operations are needed without OS thread overhead.
 * </ul>
 */
public enum ThreadMode {

  /** OS platform threads via {@code Executors.newFixedThreadPool(concurrency)}. */
  PLATFORM,

  /** JDK 21+ virtual threads via {@code Executors.newVirtualThreadPerTaskExecutor()}. */
  VIRTUAL
}
