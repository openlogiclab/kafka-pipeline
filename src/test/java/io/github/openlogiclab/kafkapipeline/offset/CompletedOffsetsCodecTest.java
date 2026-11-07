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

import static org.junit.jupiter.api.Assertions.*;

import java.util.BitSet;
import java.util.Random;
import org.junit.jupiter.api.Test;

class CompletedOffsetsCodecTest {

  @Test
  void roundTrip() {
    BitSet done = new BitSet();
    done.set(1);
    done.set(5);
    done.set(9_999);
    String meta = CompletedOffsetsCodec.encode(42, done);
    assertTrue(meta.startsWith("kp1:42:"), meta);
    assertEquals(done, CompletedOffsetsCodec.decode(meta, 42));
  }

  @Test
  void nothingDone_encodesAsEmpty() {
    assertEquals("", CompletedOffsetsCodec.encode(7, new BitSet()));
    assertTrue(CompletedOffsetsCodec.decode("", 7).isEmpty());
    assertTrue(CompletedOffsetsCodec.decode(null, 7).isEmpty());
  }

  @Test
  void differentBase_isIgnored() {
    BitSet done = new BitSet();
    done.set(3);
    String meta = CompletedOffsetsCodec.encode(100, done);
    assertTrue(CompletedOffsetsCodec.decode(meta, 101).isEmpty());
  }

  @Test
  void foreignOrCorruptMetadata_isIgnored() {
    assertTrue(CompletedOffsetsCodec.decode("written by another tool", 0).isEmpty());
    assertTrue(CompletedOffsetsCodec.decode("kp1:", 0).isEmpty());
    assertTrue(CompletedOffsetsCodec.decode("kp1:abc:xyz", 0).isEmpty());
    assertTrue(CompletedOffsetsCodec.decode("kp1:0:!!!", 0).isEmpty());
    assertTrue(CompletedOffsetsCodec.decode("kp1:0:AAAA", 0).isEmpty());
  }

  @Test
  void bitZeroIsNeverDone() {
    BitSet done = new BitSet();
    done.set(0);
    done.set(2);
    BitSet decoded = CompletedOffsetsCodec.decode(CompletedOffsetsCodec.encode(0, done), 0);
    assertFalse(decoded.get(0));
    assertTrue(decoded.get(2));
  }

  @Test
  void tooLargeToFit_encodesAsEmpty() {
    // Random bits do not compress: 100k of them is ~12 KB, well over the metadata limit.
    Random random = new Random(1);
    BitSet done = new BitSet();
    for (int i = 1; i < 100_000; i++) {
      if (random.nextBoolean()) done.set(i);
    }
    assertEquals("", CompletedOffsetsCodec.encode(0, done));
  }

  @Test
  void fullDefaultWindowOfDoneOffsets_fits() {
    // The common case: one stuck record and everything after it done.
    BitSet done = new BitSet();
    done.set(1, 10_000);
    String meta = CompletedOffsetsCodec.encode(0, done);
    assertFalse(meta.isEmpty());
    assertTrue(meta.length() <= CompletedOffsetsCodec.MAX_LENGTH);
    assertEquals(done, CompletedOffsetsCodec.decode(meta, 0));
  }
}
