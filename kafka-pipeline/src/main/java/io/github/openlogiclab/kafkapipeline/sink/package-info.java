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
 * Writing output: a {@link io.github.openlogiclab.kafkapipeline.sink.RecordProcessor} turns each
 * record into an optional output and a {@link io.github.openlogiclab.kafkapipeline.sink.RecordSink}
 * writes it. A record is acked only once its output is durable. Implement {@code RecordSink} for
 * any downstream; {@link io.github.openlogiclab.kafkapipeline.sink.SinkContext} gives each write a
 * stable record id for idempotency, and {@code RecordSink.forEach} writes several outputs per
 * record.
 *
 * <p>Public API.
 */
package io.github.openlogiclab.kafkapipeline.sink;
