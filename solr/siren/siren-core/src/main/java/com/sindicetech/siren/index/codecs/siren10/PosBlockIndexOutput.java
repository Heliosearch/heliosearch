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

package com.sindicetech.siren.index.codecs.siren10;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.codecs.block.BlockCompressor;
import com.sindicetech.siren.index.codecs.block.BlockIndexOutput;
import com.sindicetech.siren.util.ArrayUtils;

/**
 * Implementation of the {@link BlockIndexOutput} for the .pos file of the SIREn
 * postings format.
 */
public class PosBlockIndexOutput extends BlockIndexOutput {

  private final int maxBlockSize;
  private final BlockCompressor posCompressor;

  public PosBlockIndexOutput(final IndexOutput out, final int maxBlockSize,
                             final BlockCompressor posCompressor)
  throws IOException {
    super(out);
    this.posCompressor = posCompressor;
    this.maxBlockSize = maxBlockSize;
  }

  @Override
  public PosBlockWriter getBlockWriter() {
    return new PosBlockWriter();
  }

  /**
   * Implementation of the {@link BlockWriter} for the .pos file.
   *
   * <p>
   *
   * Encode and write blocks containing the term positions.
   */
  protected class PosBlockWriter extends BlockWriter {

    IntsRef posBuffer;

    BytesRef posCompressedBuffer;

    private int currentPos = 0;

    public PosBlockWriter() {
      // ensure that the input buffers has the minimum size required
      // maxBlockSize is just use as a minimum initial capacity for the buffers
      posBuffer = new IntsRef(this.getMinimumBufferSize(maxBlockSize, posCompressor.getWindowSize()));
    }

    @Override
    protected void writeHeader() throws IOException {
      // logger.debug("Write Pos header: {}", this.hashCode());
      // logger.debug("Pos header start at {}", out.getFilePointer());

      // write block sizes
      out.writeVInt(posBuffer.length);
      // write size of compressed data block
      out.writeVInt(posCompressedBuffer.length);
    }

    @Override
    protected void compress() {
      // Flip buffer before compression
      posBuffer.length = posBuffer.offset;
      posBuffer.offset = 0;

      // determine max size of compressed buffer to avoid overflow
      final int size = posCompressor.maxCompressedSize(posBuffer.length);
      posCompressedBuffer = new BytesRef(size);

      posCompressor.compress(posBuffer, posCompressedBuffer);
    }

    @Override
    protected void writeData() throws IOException {
      // logger.debug("Write Pos data: {}", this.hashCode());

      out.writeBytes(posCompressedBuffer.bytes, posCompressedBuffer.length);
    }

    /**
     * Add the term position to the buffer
     */
    public void write(final int pos) {
      if (posBuffer.offset >= posBuffer.ints.length) {
        // Take the max to ensure that buffer will be large enough
        int newLength = Math.max(posBuffer.offset + 1, posBuffer.ints.length * 3/2);
        // ensure that the buffer is large enough to accomodate the window size
        newLength = this.getMinimumBufferSize(newLength, posCompressor.getWindowSize());
        ArrayUtils.growAndCopy(posBuffer, newLength);
      }

      posBuffer.ints[posBuffer.offset++] = pos - currentPos;
      currentPos = pos;
    }

    @Override
    protected void initBlock() {
      posBuffer.offset = posBuffer.length = 0;
      this.resetCurrentPosition();
    }

    public void resetCurrentPosition() {
      currentPos = 0;
    }

    @Override
    public boolean isEmpty() {
      return posBuffer.offset == 0;
    }

    @Override
    public boolean isFull() {
      // this implementation is never full as it is synchronised with doc block
      // and grows on demand
      return false;
    }

  }

}