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
 * Abstraction over the block compression algorithm.
 */
public abstract class BlockCompressor {

  /**
   * Return the window size over the {@link IntsRef} input buffer used during
   * compression.
   * <p>
   * This is to avoid {@link ArrayIndexOutOfBoundsException} if the compressor
   * is working on a buffer which size does not match a multiple of the window
   * size.
   * <p>
   * For example, {@link AForBlockCompressor} is performing compression using
   * a window of 32 integers. In the case of short postings list with
   * 5 integers, the instantiated input buffer must be a multiple of window
   * size, i.e., 32 in this case. If the postings list would contain 33 integers
   * instead, then the instantiated input buffer should be 64.
   */
  public abstract int getWindowSize();

  /**
   * Compress the specified list of integers into a byte array.
   */
  public abstract void compress(IntsRef input, BytesRef output);

  /**
   * The maximum size in bytes of a compressed block of values
   */
  public abstract int maxCompressedSize(int blockSize);

}
