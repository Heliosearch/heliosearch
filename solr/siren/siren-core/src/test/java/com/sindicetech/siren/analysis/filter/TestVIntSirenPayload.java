/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.sindicetech.siren.analysis.filter;


import java.util.Random;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import com.sindicetech.siren.analysis.filter.VIntPayloadCodec;

public class TestVIntSirenPayload extends LuceneTestCase {

  VIntPayloadCodec codec = new VIntPayloadCodec();

  @Test
  public void testSimpleVInt()
  throws Exception {
    IntsRef ints = new IntsRef(new int[] { 12,43 }, 0, 2);
    int pos = 256;
    BytesRef bytes = codec.encode(ints, pos);
    codec.decode(bytes);

    IntsRef node = codec.getNode();
    assertEquals(ints.ints[0], node.ints[node.offset]);
    assertEquals(ints.ints[1], node.ints[node.offset + 1]);
    assertEquals(pos, codec.getPosition());

    ints = new IntsRef(new int[] { 3, 2 }, 0, 2);
    pos = 2;
    bytes = codec.encode(ints, pos);
    codec.decode(bytes);

    node = codec.getNode();
    assertEquals(ints.ints[0], node.ints[node.offset]);
    assertEquals(ints.ints[1], node.ints[node.offset + 1]);
    assertEquals(pos, codec.getPosition());

    ints = new IntsRef(new int[] { 0, 1 }, 0, 2);
    pos = 0;
    bytes = codec.encode(ints, pos);
    codec.decode(bytes);

    node = codec.getNode();
    assertEquals(ints.ints[0], node.ints[node.offset]);
    assertEquals(ints.ints[1], node.ints[node.offset + 1]);
    assertEquals(pos, codec.getPosition());
  }

  @Test
  public void testRandomVInt2()
  throws Exception {
    final Random r = LuceneTestCase.random();
    for (int i = 0; i < 10000; i++) {
      final int value1 = r.nextInt(Integer.MAX_VALUE);
      final int value2 = r.nextInt(Integer.MAX_VALUE);

      final IntsRef ints = new IntsRef(new int[] { value1,value2 }, 0, 2);
      final int pos = r.nextInt(Integer.MAX_VALUE);
      final BytesRef bytes = codec.encode(ints, pos);
      codec.decode(bytes);

      final IntsRef node = codec.getNode();
      assertEquals(ints.ints[0], node.ints[node.offset]);
      assertEquals(ints.ints[1], node.ints[node.offset + 1]);
      assertEquals(pos, codec.getPosition());
    }
  }

  @Test
  public void testRandomVInt3()
  throws Exception {
    final Random r = LuceneTestCase.random();
    for (int i = 0; i < 10000; i++) {
      final int value1 = r.nextInt(Integer.MAX_VALUE);
      final int value2 = r.nextInt(Integer.MAX_VALUE);
      final int value3 = r.nextInt(Integer.MAX_VALUE);

      final IntsRef ints = new IntsRef(new int[] { value1,value2,value3 }, 0, 3);
      final int pos = r.nextInt(Integer.MAX_VALUE);
      final BytesRef bytes = codec.encode(ints, pos);
      codec.decode(bytes);

      final IntsRef node = codec.getNode();
      assertEquals(ints.ints[0], node.ints[node.offset]);
      assertEquals(ints.ints[1], node.ints[node.offset + 1]);
      assertEquals(ints.ints[2], node.ints[node.offset + 2]);
      assertEquals(pos, codec.getPosition());
    }
  }

}
