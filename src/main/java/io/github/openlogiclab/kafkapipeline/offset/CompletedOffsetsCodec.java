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
package io.github.openlogiclab.kafkapipeline.offset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.BitSet;
import java.util.zip.Deflater;
import java.util.zip.InflaterInputStream;

/**
 * Encodes the offsets that finished beyond the committed offset into commit metadata, so a consumer
 * that takes over the partition (after a crash, restart or rebalance) can skip them instead of
 * processing them again.
 *
 * <p>Format: {@code kp1:<base>:<payload>}, where {@code base} is the committed offset the bits are
 * relative to, and {@code payload} is the URL-safe Base64 of the deflated {@link
 * BitSet#toByteArray()} in which bit {@code i} means offset {@code base + i} is done. The base is
 * part of the string so a decoder can tell the bits belong to the offset it is starting from.
 */
final class CompletedOffsetsCodec {

  static final String PREFIX = "kp1:";

  /**
   * Longest metadata this codec produces. Kafka rejects commits whose metadata is longer than the
   * broker's {@code offset.metadata.max.bytes} (4096 by default); a set that does not fit is not
   * encoded at all, which only means those records are processed again after a restart.
   */
  static final int MAX_LENGTH = 4000;

  /**
   * Offsets further than this past the committed one are never recorded, so the bitmap stays at
   * most 128 KB per partition even when offsets are sparse (compacted topics). Finished records
   * beyond it are simply processed again.
   */
  static final int MAX_SPAN = 1 << 20;

  /** Upper bound on the decoded bitmap, so a corrupt or hostile string cannot exhaust memory. */
  private static final int MAX_DECODED_BYTES = MAX_SPAN / 8;

  private CompletedOffsetsCodec() {}

  /**
   * Encodes {@code done} relative to {@code base}.
   *
   * @return the metadata string, or an empty string when nothing is done or it would not fit
   */
  static String encode(long base, BitSet done) {
    if (done.isEmpty()) {
      return "";
    }
    byte[] raw = done.toByteArray();
    Deflater deflater = new Deflater(Deflater.BEST_SPEED);
    try {
      deflater.setInput(raw);
      deflater.finish();
      ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, raw.length / 4));
      byte[] buf = new byte[1024];
      while (!deflater.finished()) {
        int n = deflater.deflate(buf);
        out.write(buf, 0, n);
        if (out.size() > MAX_LENGTH) {
          return "";
        }
      }
      String encoded =
          PREFIX
              + base
              + ':'
              + Base64.getUrlEncoder().withoutPadding().encodeToString(out.toByteArray());
      return encoded.length() <= MAX_LENGTH ? encoded : "";
    } finally {
      deflater.end();
    }
  }

  /**
   * Decodes metadata written by {@link #encode}.
   *
   * @param metadata the committed metadata, possibly null or written by someone else
   * @param expectedBase the offset the consumer will start from
   * @return the done offsets relative to {@code expectedBase}; empty when the metadata is missing,
   *     unreadable, or relative to a different offset
   */
  static BitSet decode(String metadata, long expectedBase) {
    if (metadata == null || !metadata.startsWith(PREFIX)) {
      return new BitSet();
    }
    int sep = metadata.indexOf(':', PREFIX.length());
    if (sep < 0) {
      return new BitSet();
    }
    try {
      long base = Long.parseLong(metadata.substring(PREFIX.length(), sep));
      if (base != expectedBase) {
        return new BitSet();
      }
      byte[] compressed = Base64.getUrlDecoder().decode(metadata.substring(sep + 1));
      byte[] raw;
      try (InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(compressed))) {
        // Truncated input fails with EOFException, a preset dictionary with ZipException.
        raw = in.readNBytes(MAX_DECODED_BYTES + 1);
      }
      if (raw.length > MAX_DECODED_BYTES) {
        return new BitSet();
      }
      BitSet done = BitSet.valueOf(raw);
      // Offset `base` itself is the first unfinished record, so it can never be done.
      done.clear(0);
      return done;
    } catch (IllegalArgumentException | IOException e) {
      return new BitSet();
    }
  }
}
