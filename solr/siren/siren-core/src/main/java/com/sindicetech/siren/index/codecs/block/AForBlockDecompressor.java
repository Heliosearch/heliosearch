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

import com.sindicetech.siren.index.codecs.block.AForFrameDecompressor.FrameDecompressor;

/**
 * Implementation of {@link BlockDecompressor} based on the Adaptive Frame Of
 * Reference algorithm.
 *
 * @see AForBlockCompressor
 */
public class AForBlockDecompressor extends BlockDecompressor {

  protected final int[] frameLengths = AForFrameDecompressor.frameLengths;
  protected final int[] frameSizes = AForFrameDecompressor.frameSizes;
  protected final FrameDecompressor[] decompressors = AForFrameDecompressor.decompressors;

  @Override
  public void decompress(final BytesRef input, final IntsRef output) {
    assert output.ints.length % 32 == 0;
    final byte[] compressedArray = input.bytes;

    while (input.offset < input.length) {
      this.decompressors[compressedArray[input.offset]].decompress(input, output);
    }

    // flip buffer
    input.offset = 0;
    output.length = output.offset;
    output.offset = 0;
  }

  @Override
  public int skip(BytesRef input, int n) {
    int skipped = 0;
    final byte[] compressedArray = input.bytes;

    while (input.offset < input.length) {
      byte bitFrame = compressedArray[input.offset];
      int frameSize = frameSizes[bitFrame];
      if (skipped + frameSize > n) {
        return skipped;
      }
      skipped += frameSize;
      input.offset += frameLengths[bitFrame] + 1;
    }
    return skipped;
  }

  @Override
  public int getWindowSize() {
    return AForBlockCompressor.MAX_FRAME_SIZE;
  }

}
