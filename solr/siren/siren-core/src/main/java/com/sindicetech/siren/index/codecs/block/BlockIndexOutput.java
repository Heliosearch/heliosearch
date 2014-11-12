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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Abstract API to encode a block-based posting format.
 *
 * <p>
 *
 * This class is not thread-safe and not safe for concurrent file access. It
 * must not be used to encode multiple blocks concurrently. In the current
 * implementation of Lucene, terms are processed sequentially during the
 * creation of a new index segment. It is ensured that (1) one instance of this
 * class is always used by one single thread, and (2) one instance of this class
 * is always encoding postings one term at a time.
 */
public abstract class BlockIndexOutput implements Closeable {

  protected final IndexOutput out;

  protected static final Logger logger = LoggerFactory.getLogger(BlockIndexOutput.class);

  public BlockIndexOutput(final IndexOutput out) {
    this.out = out;
  }

  /**
   * Instantiates a new block index.
   */
  public Index index() {
    return new Index();
  }

  /**
   * This class stores the file pointer of an {@link IndexOutput}.
   */
  public class Index {

    long fp;
    long lastFP;

    public void mark() throws IOException {
      fp = out.getFilePointer();
    }

    public void copyFrom(final BlockIndexOutput.Index other, final boolean copyLast) {
      final Index idx = other;
      fp = idx.fp;
      if (copyLast) {
        lastFP = fp;
      }
    }

    public void write(final DataOutput indexOut, final boolean absolute)
    throws IOException {
      // logger.debug("Write index at {}", fp);
      if (absolute) {
        indexOut.writeVLong(fp);
      }
      else {
        indexOut.writeVLong(fp - lastFP);
      }
      lastFP = fp;
    }

    @Override
    public String toString() {
      return "fp=" + fp;
    }
  }

  /**
   * Writes a codec footer, which records both a checksum algorithm ID and a checksum, using
   * {@link org.apache.lucene.codecs.CodecUtil#writeFooter(org.apache.lucene.store.IndexOutput)}
   *
   * @throws java.io.IOException If there is an I/O error writing to the underlying medium.
   */
  public void writeFooter() throws IOException {
    CodecUtil.writeFooter(out);
  }

  public void close() throws IOException {
    out.close();
  }

  /**
   * Create a new {@link BlockWriter} associated to this
   * {@link BlockIndexOutput}.
   *
   * <p>
   *
   * You should ensure to flush all {@link BlockWriter} before closing the
   * {@link BlockIndexOutput}.
   *
   * <p>
   *
   * More than one {@link BlockWriter} can be instantiated by a
   * {@link BlockIndexOutput}. Usually one writer is instantiated for each term.
   */
  public abstract BlockWriter getBlockWriter();

  /**
   * Abstraction over the writer of the blocks of the postings file.
   *
   * <p>
   *
   * The abstraction provides an interface to write and flush blocks. Subclasses
   * must implement the encoding of the block header and the encoding of
   * the block data.
   */
  protected abstract class BlockWriter {

    /**
     * Flush of pending data block to the output file.
     */
    public void flush() throws IOException {
      // Flush only if the block is non empty
      if (!this.isEmpty()) {
        this.writeBlock();
      }
    }

    /**
     * Write data block to the output file with the following sequence of
     * operations:
     * <ul>
     * <li> Compress the data
     * <li> Write block header (as header can depend on statistic computed
     * from data compression)
     * <li> Write compressed data block
     * <li> Reset writer for new block
     * </ul>
     */
    protected void writeBlock() throws IOException {
      this.compress();
      this.writeHeader();
      this.writeData();
      this.initBlock();
    }

    public abstract boolean isEmpty();

    public abstract boolean isFull();

    /**
     * Compress the data block
     */
    protected abstract void compress();

    /**
     * Write block header to the output file
     */
    protected abstract void writeHeader() throws IOException;

    /**
     * Write compressed data block to the output file
     */
    protected abstract void writeData() throws IOException;

    /**
     * Init writer for new block
     */
    protected abstract void initBlock();

    /**
     * Compute the minimum size of a buffer based on the required size and
     * the compression window size.
     */
    protected int getMinimumBufferSize(final int bufferSize, final int windowSize) {
      return (int) Math.ceil((float) bufferSize / (float) windowSize) * windowSize;
    }

  }

}
