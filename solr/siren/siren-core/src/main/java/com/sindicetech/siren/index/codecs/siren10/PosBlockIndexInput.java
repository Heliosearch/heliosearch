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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.codecs.block.BlockDecompressor;
import com.sindicetech.siren.index.codecs.block.BlockIndexInput;
import com.sindicetech.siren.util.ArrayUtils;

import java.io.IOException;

/**
 * Implementation of the {@link BlockIndexInput} for the .pos file of the SIREn
 * postings format.
 */
public class PosBlockIndexInput extends BlockIndexInput {

  protected BlockDecompressor posDecompressor;

  public PosBlockIndexInput(final IndexInput in, final BlockDecompressor posDecompressor)
  throws IOException {
    super(in);
    this.posDecompressor = posDecompressor;
  }

  @Override
  public PosBlockReader getBlockReader() {
    // Clone index input. A cloned index input does not need to be closed
    // by the block reader, as the underlying stream will be closed by the
    // input it was cloned from
    return new PosBlockReader(in.clone());
  }

  /**
   * Implementation of the {@link BlockReader} for the .pos file.
   *
   * <p>
   *
   * Read and decode blocks containing the the term positions.
   */
  protected class PosBlockReader extends BlockReader {

    int posBlockSize;
    IntsRef posBuffer = new IntsRef();

    boolean posReadPending = true;

    int posCompressedBufferLength;
    BytesRef posCompressedBuffer = new BytesRef();

    private int currentPos = 0;

    private PosBlockReader(final IndexInput in) {
      super(in);
      // ensure that the output buffers has the minimum size required
      posBuffer = ArrayUtils.grow(posBuffer, posDecompressor.getWindowSize());
    }

    @Override
    protected void readHeader() throws IOException {
      // logger.debug("Decode Pos header: {}", this.hashCode());
      // logger.debug("Pos header start at {}", in.getFilePointer());

      // read blockSize and check buffer size
      posBlockSize = in.readVInt();
      // ensure that the output buffer has the minimum size required
      final int posLength = this.getMinimumBufferSize(posBlockSize, posDecompressor.getWindowSize());
      posBuffer = ArrayUtils.grow(posBuffer, posLength);
      // logger.debug("Read Pos block size: {}", posBlockSize);

      // read size of each compressed data block and check buffer size
      posCompressedBufferLength = in.readVInt();
      posCompressedBuffer = ArrayUtils.grow(posCompressedBuffer, posCompressedBufferLength);
      posReadPending = true;
    }

    @Override
    protected void skipData() throws IOException {
      long size = 0;
      if (posReadPending) {
        size += posCompressedBufferLength;
      }
      this.seek(in.getFilePointer() + size);
      // logger.debug("Skip Pos data: {}", in.getFilePointer() + size);
    }

    /**
     * Decode and return the next position of the current block.
     */
    public int nextPosition() throws IOException {
      if (posReadPending) {
        this.decodePositions();
      }
      assert posBuffer.offset <= posBuffer.length;
      return currentPos = posBuffer.ints[posBuffer.offset++] + currentPos;
    }

    private void decodePositions() throws IOException {
      // logger.debug("Decode Pos: {}", this.hashCode());

      in.readBytes(posCompressedBuffer.bytes, 0, posCompressedBufferLength);
      posCompressedBuffer.offset = 0;
      posCompressedBuffer.length = posCompressedBufferLength;
      posDecompressor.decompress(posCompressedBuffer, posBuffer);
      // set length limit based on block size, as certain decompressor with
      // large window size can set it larger than the blockSize, e.g., AFor
      posBuffer.length = posBlockSize;

      posReadPending = false;
    }

    @Override
    public boolean isExhausted() {
      return posBuffer.offset >= posBuffer.length;
    }

    @Override
    protected void initBlock() {
      posBuffer.offset = posBuffer.length = 0;
      this.resetCurrentPosition();

      posReadPending = true;

      posCompressedBufferLength = 0;
    }

    public void resetCurrentPosition() {
      currentPos = 0;
    }

  }

}
