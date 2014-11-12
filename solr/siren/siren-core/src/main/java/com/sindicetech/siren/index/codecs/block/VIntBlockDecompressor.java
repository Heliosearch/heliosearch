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

package com.sindicetech.siren.index.codecs.block;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/**
 * Implementation of the {@link BlockDecompressor} based on the Variable Integer
 * encoding algorithm.
 */
public class VIntBlockDecompressor extends BlockDecompressor {

  @Override
  public void decompress(final BytesRef input, final IntsRef output) {
    final byte[] compressedData = input.bytes;
    final int[] unCompressedData = output.ints;

    while (input.offset < input.length) {
      byte b = compressedData[input.offset++];
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = compressedData[input.offset++];
        i |= (b & 0x7F) << shift;
      }
      unCompressedData[output.offset++] = i;
    }

    // flip buffer
    input.offset = 0;
    output.length = output.offset;
    output.offset = 0;
  }

  @Override
  public int skip(BytesRef input, int n) {
    int skipped = 0;
    final byte[] compressedData = input.bytes;

    while (input.offset < input.length && skipped < n) {
      byte b = compressedData[input.offset++];
      skipped++;
      while ((b & 0x80) != 0) {
        b = compressedData[input.offset++];
      }
    }

    return skipped;
  }

  @Override
  public int getWindowSize() {
    return 1;
  }

}
