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
 * Global backpressure settings and the sensor SPI. When any sensor reports {@code THROTTLE}, the
 * pipeline pauses fetching on every partition. The built-in sensors are configured with {@link
 * io.github.openlogiclab.kafkapipeline.backpressure.RecordBackpressureConfig}, {@link
 * io.github.openlogiclab.kafkapipeline.backpressure.ByteBackpressureConfig} and {@link
 * io.github.openlogiclab.kafkapipeline.backpressure.HeapBackpressureConfig}; add your own by
 * implementing {@link io.github.openlogiclab.kafkapipeline.backpressure.BackpressureSensor}.
 *
 * <p>Public API. The implementation lives in {@code internal.backpressure}.
 */
package io.github.openlogiclab.kafkapipeline.backpressure;
