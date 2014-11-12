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
 * Abstraction over the block decompression algorithm.
 */
public abstract class BlockDecompressor {

  /**
   * Return the window size over the {@link IntsRef} output buffer used during
   * decompression.
   * <p>
   * This is to avoid {@link ArrayIndexOutOfBoundsException} if the decompressor
   * is working on a buffer which size does not match a multiple of the window
   * size.
   * <p>
   * For example, {@link AForBlockDecompressor} is performing decompression using
   * a window of 32 integers. In the case of short postings list with
   * 5 integers, the instantiated output buffer must be a multiple of window
   * size, i.e., 32 in this case. If the postings list would contain 33 integers
   * instead, then the instantiated output buffer should be 64.
   */
  public abstract int getWindowSize();

  /**
   * Decompress the specified byte array into a list of integers.
   */
  public abstract void decompress(BytesRef input, IntsRef output);

  /**
   * Try to skip the first n integers in the block. Depending on the implementation, it might not be able possible to
   * skip exactly the n integers. Therefore, the method must return the number of skipped integers.
   *
   * <p>
   *  This method should be called before {@link #decompress(org.apache.lucene.util.BytesRef, org.apache.lucene.util.IntsRef)}.
   * </p>
   */
  public abstract int skip(BytesRef input, int n);

}
