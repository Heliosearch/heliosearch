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
 * Implementation of the {@link BlockIndexInput} for the .nod file of the SIREn
 * postings format.
 */
public class NodBlockIndexInput extends BlockIndexInput {

  protected BlockDecompressor nodDecompressor;

  public NodBlockIndexInput(final IndexInput in, final BlockDecompressor nodDecompressor)
  throws IOException {
    super(in);
    this.nodDecompressor = nodDecompressor;
  }

  @Override
  public NodBlockReader getBlockReader() {
    // Clone index input. A cloned index input does not need to be closed
    // by the block reader, as the underlying stream will be closed by the
    // input it was cloned from
    return new NodBlockReader(in.clone());
  }

  /**
   * Implementation of the {@link BlockReader} for the .nod file.
   *
   * <p>
   *
   * Read and decode blocks containing the the node labels and term frequencies.
   */
  protected class NodBlockReader extends BlockReader {

    int nodLenBlockSize;
    IntsRef nodLenBuffer = new IntsRef();
    int nodBlockSize;
    IntsRef nodBuffer = new IntsRef();
    int termFreqBlockSize;
    IntsRef termFreqBuffer = new IntsRef();

    /**
     * Used to slice the nodBuffer and disclose only the subset containing
     * information about the current node.
     */
    private final IntsRef currentNode = new IntsRef();

    boolean nodLenReadPending = true;
    boolean nodReadPending = true;
    boolean termFreqReadPending = true;

    int nodLenCompressedBufferLength;
    BytesRef nodLenCompressedBuffer = new BytesRef();
    int nodCompressedBufferLength;
    BytesRef nodCompressedBuffer = new BytesRef();
    int termFreqCompressedBufferLength;
    BytesRef termFreqCompressedBuffer = new BytesRef();

    private NodBlockReader(final IndexInput in) {
      super(in);
      // ensure that the output buffers has the minimum size required
      nodLenBuffer = ArrayUtils.grow(nodLenBuffer, nodDecompressor.getWindowSize());
      nodBuffer = ArrayUtils.grow(nodBuffer, nodDecompressor.getWindowSize());
      termFreqBuffer = ArrayUtils.grow(termFreqBuffer, nodDecompressor.getWindowSize());
    }

    @Override
    protected void readHeader() throws IOException {
      // logger.debug("Read Nod header: {}", this.hashCode());
      // logger.debug("Nod header start at {}", in.getFilePointer());

      // read blockSize and check buffer size
      nodLenBlockSize = in.readVInt();
      // ensure that the output buffer has the minimum size required
      final int nodLenBufferLength = this.getMinimumBufferSize(nodLenBlockSize, nodDecompressor.getWindowSize());
      nodLenBuffer = ArrayUtils.grow(nodLenBuffer, nodLenBufferLength);
      // logger.debug("Read Nod length block size: {}", nodLenblockSize);

      nodBlockSize = in.readVInt();
      // ensure that the output buffer has the minimum size required
      final int nodBufferLength = this.getMinimumBufferSize(nodBlockSize, nodDecompressor.getWindowSize());
      nodBuffer = ArrayUtils.grow(nodBuffer, nodBufferLength);
      // logger.debug("Read Nod block size: {}", nodBlockSize);

      termFreqBlockSize = in.readVInt();
      // ensure that the output buffer has the minimum size required
      final int termFreqBufferLength = this.getMinimumBufferSize(termFreqBlockSize, nodDecompressor.getWindowSize());
      termFreqBuffer = ArrayUtils.grow(termFreqBuffer, termFreqBufferLength);
      // logger.debug("Read Term Freq In Node block size: {}", termFreqblockSize);

      // read size of each compressed data block and check buffer size
      nodLenCompressedBufferLength = in.readVInt();
      nodLenCompressedBuffer = ArrayUtils.grow(nodLenCompressedBuffer, nodLenCompressedBufferLength);
      nodLenReadPending = true;

      nodCompressedBufferLength = in.readVInt();
      nodCompressedBuffer = ArrayUtils.grow(nodCompressedBuffer, nodCompressedBufferLength);
      nodReadPending = true;

      termFreqCompressedBufferLength = in.readVInt();
      termFreqCompressedBuffer = ArrayUtils.grow(termFreqCompressedBuffer, termFreqCompressedBufferLength);
      termFreqReadPending = true;

      // copy reference of node buffer
      currentNode.ints = nodBuffer.ints;
    }

    @Override
    protected void skipData() throws IOException {
      long size = 0;
      if (nodLenReadPending) {
        size += nodLenCompressedBufferLength;
      }
      if (nodReadPending) {
        size += nodCompressedBufferLength;
      }
      if (termFreqReadPending) {
        size += termFreqCompressedBufferLength;
      }
      this.seek(in.getFilePointer() + size);
      // logger.debug("Skip Nod data: {}", in.getFilePointer() + size);
    }

    private void decodeNodeLengths() throws IOException {
      // logger.debug("Decode Nodes Length: {}", this.hashCode());
      // logger.debug("Decode Nodes Length at {}", in.getFilePointer());
      in.readBytes(nodLenCompressedBuffer.bytes, 0, nodLenCompressedBufferLength);
      nodLenCompressedBuffer.offset = 0;
      nodLenCompressedBuffer.length = nodLenCompressedBufferLength;
      nodDecompressor.decompress(nodLenCompressedBuffer, nodLenBuffer);
      // set length limit based on block size, as certain decompressor with
      // large window size can set it larger than the blockSize, e.g., AFor
      nodLenBuffer.length = nodLenBlockSize;

      nodLenReadPending = false;
    }

    private void decodeNodes() throws IOException {
      // logger.debug("Decode Nodes: {}", this.hashCode());
      // logger.debug("Decode Nodes at {}", in.getFilePointer());
      in.readBytes(nodCompressedBuffer.bytes, 0, nodCompressedBufferLength);
      nodCompressedBuffer.offset = 0;
      nodCompressedBuffer.length = nodCompressedBufferLength;
      nodDecompressor.decompress(nodCompressedBuffer, nodBuffer);
      // set length limit based on block size, as certain decompressor with
      // large window size can set it larger than the blockSize, e.g., AFor
      nodBuffer.length = nodBlockSize;

      nodReadPending = false;
    }


    private int skipAndDecodeNodes(int nNodes) throws IOException {
      // logger.debug("Decode Nodes: {}", this.hashCode());
      // logger.debug("Decode Nodes at {}", in.getFilePointer());
      in.readBytes(nodCompressedBuffer.bytes, 0, nodCompressedBufferLength);
      nodCompressedBuffer.offset = 0;
      nodCompressedBuffer.length = nodCompressedBufferLength;
      int skipped = nodDecompressor.skip(nodCompressedBuffer, nNodes);
      nodDecompressor.decompress(nodCompressedBuffer, nodBuffer);
      // set length limit based on block size, as certain decompressor with
      // large window size can set it larger than the blockSize, e.g., AFor
      nodBuffer.length = nodBlockSize;

      nodReadPending = false;

      return skipped;
    }

    private void decodeTermFreqs() throws IOException {
      // logger.debug("Decode Term Freq in Node: {}", this.hashCode());
      // logger.debug("Decode Term Freq in Node at {}", in.getFilePointer());
      in.readBytes(termFreqCompressedBuffer.bytes, 0, termFreqCompressedBufferLength);
      termFreqCompressedBuffer.offset = 0;
      termFreqCompressedBuffer.length = termFreqCompressedBufferLength;
      nodDecompressor.decompress(termFreqCompressedBuffer, termFreqBuffer);
      // set length limit based on block size, as certain decompressor with
      // large window size can set it larger than the blockSize, e.g., AFor
      termFreqBuffer.length = termFreqBlockSize;

      termFreqReadPending = false;
    }

    /**
     * Decode and return the next node label of the current block.
     *
     * <p>
     *
     * The {@link IntsRef} returned is a slice of the uncompressed node block.
     */
    public IntsRef nextNode() throws IOException {
      // ensure that node lengths are decoded
      if (nodLenReadPending) {
        this.decodeNodeLengths();
      }
      // ensure that node ids are decoded
      if (nodReadPending) {
        this.decodeNodes();
      }
      // decode delta
      this.deltaDecoding();
      return currentNode;
    }

    /**
     * Expert: Skip the decoding of a given number of nodes. Usually, the number of nodes is a sum of node frequencies
     * returned by {@link com.sindicetech.siren.index.codecs.siren10.DocsFreqBlockIndexInput}.
     *
     * <p>
     *   This method must be used to skip range of nodes from one or more document. If this method is called after
     *   having partially read the node ids of a document, the consequences are unknown. Similarly, if the target of the
     *   skip is not the first node ids of a document, the consequences are unknown.
     * </p>
     *
     * <p>
     *   This is used by {@link com.sindicetech.siren.index.codecs.siren10.Siren10PostingsReader.Siren10DocsNodesAndPositionsEnum}
     *   to skip pending node ids after a document skip.
     * </p>
     *
     * @param nNodes The number of nodes to skip
     */
    void skipNodes(int nNodes) throws IOException {
      // ensure that node lengths are decoded
      if (nodLenReadPending) {
        this.decodeNodeLengths();
      }

      // Accumulate node lengths
      int lenAccumulator = 0;
      for (int i = 0; i < nNodes; i++) {
        // increment length by one
        lenAccumulator += nodLenBuffer.ints[nodLenBuffer.offset++] + 1;
      }

      // skip and decode node ids
      if (nodReadPending) {
        int skipped = this.skipAndDecodeNodes(lenAccumulator);
        lenAccumulator -= skipped;
      }

      // Skip node ids
      // can safely skip node ids, since delta decoding is reset after each document
      nodBuffer.offset += lenAccumulator;
    }

    /**
     * Decode delta of the node.
     * <p>
     * If a new doc has been read (currentNode.length == 0), then update currentNode
     * offset and length. Otherwise, perform delta decoding.
     * <p>
     * Perform delta decoding while current node id and previous node id are
     * equals.
     */
    private final void deltaDecoding() {
      final int[] nodBufferInts = nodBuffer.ints;
      // increment length by one
      final int nodLength = nodLenBuffer.ints[nodLenBuffer.offset++] + 1;
      final int nodOffset = nodBuffer.offset;
      final int nodEnd = nodOffset + nodLength;

      final int currentNodeOffset = currentNode.offset;
      final int currentNodeEnd = currentNodeOffset + currentNode.length;

      for (int i = nodOffset, j = currentNodeOffset;
           i < nodEnd && j < currentNodeEnd; i++, j++) {
        nodBufferInts[i] += nodBufferInts[j];
        // if node ids are different, then stop decoding
        if (nodBufferInts[i] != nodBufferInts[j]) {
          break;
        }
      }

      // increment node buffer offset
      nodBuffer.offset += nodLength;
      // update last node offset and length
      currentNode.offset = nodOffset;
      currentNode.length = nodLength;
    }

    /**
     * Decode and return the next term frequency of the current block.
     */
    public int nextTermFreqInNode() throws IOException {
      if (termFreqReadPending) {
        this.decodeTermFreqs();
      }
      // increment freq by one
      return termFreqBuffer.ints[termFreqBuffer.offset++] + 1;
    }

    @Override
    public boolean isExhausted() {
      return nodLenBuffer.offset >= nodLenBuffer.length;
    }

    @Override
    public void initBlock() {
      nodLenBuffer.offset = nodLenBuffer.length = 0;
      nodBuffer.offset = nodBuffer.length = 0;
      termFreqBuffer.offset = termFreqBuffer.length = 0;
      this.resetCurrentNode();

      nodLenReadPending = true;
      nodReadPending = true;
      termFreqReadPending = true;

      nodLenCompressedBufferLength = 0;
      nodCompressedBufferLength = 0;
      termFreqCompressedBufferLength = 0;
    }

    public void resetCurrentNode() {
      currentNode.offset = currentNode.length = 0;
    }

  }

}
