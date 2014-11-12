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
 * Implementation of the {@link BlockCompressor} based on the Variable Integer
 * encoding algorithm.
 */
public class VIntBlockCompressor extends BlockCompressor {

  /** Size of header in buffer */
  protected final int HEADER_SIZE = 0;

  /**
   * Compress the uncompressed data into the buffer using variable integer
   * encoding technique. All uncompressed integers are stored sequentially in
   * compressed form in the buffer after the header.
   * <p>
   * No header is stored.
   */
  @Override
  public void compress(final IntsRef input, final BytesRef output) {
    final int[] uncompressedData = input.ints;
    final byte[] compressedData = output.bytes;

    for (int i = 0; i < input.length; i++) {
      int value = uncompressedData[i];
      while ((value & ~0x7F) != 0) {
        compressedData[output.offset++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
      compressedData[output.offset++] = (byte) value;
    }

    // flip buffer
    output.length = output.offset;
    output.offset = 0;
  }

  @Override
  public int maxCompressedSize(final int arraySize) {
    return HEADER_SIZE + (5 * arraySize);
  }

  @Override
  public int getWindowSize() {
    return 1;
  }

}
