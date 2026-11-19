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
/**
 * Hands polled records to workers in per-record mode. {@link
 * io.github.openlogiclab.kafkapipeline.dispatch.RecordDispatcher} keeps one lane per partition: a
 * FIFO lane, or a keyed lane that keeps one record per key in flight. The poll thread adds records;
 * worker threads take and release them. Ordering ({@code KEY}, {@code PARTITION}) and the
 * per-partition concurrency limit are enforced here.
 *
 * <p>Internal: not exported from the module.
 */
package io.github.openlogiclab.kafkapipeline.dispatch;
