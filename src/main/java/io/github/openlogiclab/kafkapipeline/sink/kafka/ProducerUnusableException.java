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
package io.github.openlogiclab.kafkapipeline.sink.kafka;

import org.apache.kafka.common.KafkaException;

/**
 * The producer rejected a send because it can no longer send anything (for example it was closed by
 * its owner). Classified as {@code FATAL}: every later write would fail the same way.
 */
final class ProducerUnusableException extends KafkaException {

  private static final long serialVersionUID = 1L;

  ProducerUnusableException(IllegalStateException cause) {
    super("The producer cannot send any more: " + cause.getMessage(), cause);
  }
}
