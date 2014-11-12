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

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.analysis.filter.VIntPayloadCodec;
import com.sindicetech.siren.index.MappingMultiDocsNodesAndPositionsEnum;
import com.sindicetech.siren.index.codecs.block.BlockIndexOutput;

import java.io.IOException;
import java.util.List;

/**
 * Writes the document identifiers, node frequencies, node labels, term
 * frequencies, term positions and block skip data.
 */
public class Siren10PostingsWriter extends PostingsWriterBase {

  final static String CODEC = "Siren10PostingsWriter";

  // Increment version to change it:
  final static int VERSION_START = 0;
  final static int VERSION_META_ARRAY = 1;
  final static int VERSION_CHECKSUM = 2;
  final static int VERSION_CURRENT = VERSION_CHECKSUM;

  DocsFreqBlockIndexOutput                     docOut;
  DocsFreqBlockIndexOutput.DocsFreqBlockWriter docWriter;
  DocsFreqBlockIndexOutput.Index               docIndex;

  NodBlockIndexOutput                          nodOut;
  NodBlockIndexOutput.NodBlockWriter           nodWriter;
  NodBlockIndexOutput.Index                    nodIndex;

  PosBlockIndexOutput                          posOut;
  PosBlockIndexOutput.PosBlockWriter           posWriter;
  PosBlockIndexOutput.Index                    posIndex;

  IndexOutput                                  skipOut;

  final Siren10SkipListWriter skipWriter;

  /**
   * Expert: The fraction of blocks stored in skip tables,
   * used to accelerate {@link DocsEnum#advance(int)}.  Larger values result in
   * smaller indexes, greater acceleration, but fewer accelerable cases, while
   * smaller values result in bigger indexes, less acceleration and more
   * accelerable cases.
   */
  final int blockSkipInterval;
  static final int DEFAULT_BLOCK_SKIP_INTERVAL = 2;

  /**
   * Expert: minimum block to write any skip data at all
   */
  final int blockSkipMinimum;

  /**
   * Expert: maximum block size allowed.
   */
  final int maxBlockSize;

  /**
   * Expert: The maximum number of skip levels. Smaller values result in
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  final int maxSkipLevels = 10;

  final int totalNumDocs;

  IndexOptions indexOptions;

  FieldInfo fieldInfo;

  int blockCount;

  Siren10TermState lastState;
  long lastSkipFP;

  protected static final Logger logger = LoggerFactory.getLogger(Siren10PostingsWriter.class);

  public Siren10PostingsWriter(final SegmentWriteState state,
                               final Siren10BlockStreamFactory factory)
  throws IOException {
    this(state, DEFAULT_BLOCK_SKIP_INTERVAL, factory);
  }

  public Siren10PostingsWriter(final SegmentWriteState state,
                               final int blockSkipInterval,
                               final Siren10BlockStreamFactory factory)
  throws IOException {
    nodOut = null;
    nodIndex = null;
    posOut = null;
    posIndex = null;
    boolean success = false;

    try {
      this.blockSkipInterval = blockSkipInterval;
      this.blockSkipMinimum = blockSkipInterval; /* set to the same for now */

      final String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, Siren10PostingsFormat.DOC_EXTENSION);
      docOut = factory.createDocsFreqOutput(state.directory, docFileName, state.context);
      docWriter = docOut.getBlockWriter();
      docIndex = docOut.index();

      this.maxBlockSize = docWriter.getMaxBlockSize();

      final String nodFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, Siren10PostingsFormat.NOD_EXTENSION);
      nodOut = factory.createNodOutput(state.directory, nodFileName, state.context);
      nodWriter = nodOut.getBlockWriter();
      nodIndex = nodOut.index();

      final String posFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, Siren10PostingsFormat.POS_EXTENSION);
      posOut = factory.createPosOutput(state.directory, posFileName, state.context);
      posWriter = posOut.getBlockWriter();
      posIndex = posOut.index();

      final String skipFileName = IndexFileNames.segmentFileName(state.segmentInfo.name,
        state.segmentSuffix, Siren10PostingsFormat.SKIP_EXTENSION);
      skipOut = state.directory.createOutput(skipFileName, state.context);

      totalNumDocs = state.segmentInfo.getDocCount();

      // EStimate number of blocks that will be written
      final int numBlocks = (int) Math.ceil(totalNumDocs / (double) docWriter.getMaxBlockSize());
      skipWriter = new Siren10SkipListWriter(blockSkipInterval, maxSkipLevels,
        numBlocks, docOut);
      docWriter.setNodeBlockIndex(nodIndex);
      docWriter.setPosBlockIndex(posIndex);

      success = true;
    }
    finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, skipOut, nodOut, posOut);
      }
    }
  }

  @Override
  public void init(final IndexOutput termsOut) throws IOException {
    CodecUtil.writeHeader(termsOut, CODEC, VERSION_CURRENT);
    termsOut.writeInt(blockSkipInterval);                // write skipInterval
    termsOut.writeInt(maxSkipLevels);               // write maxSkipLevels
    termsOut.writeInt(blockSkipMinimum);                 // write skipMinimum
    termsOut.writeInt(maxBlockSize);                 // write maxBlockSize
  }

  @Override
  public BlockTermState newTermState() {
    return new Siren10TermState();
  }

  @Override
  public void startTerm() throws IOException {
    docIndex.mark();
    nodIndex.mark();
    posIndex.mark();

    skipWriter.resetSkip(docIndex);
  }

  // Currently, this instance is re-used across fields, so
  // our parent calls setField whenever the field changes
  @Override
  public int setField(final FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.indexOptions = fieldInfo.getIndexOptions();
    if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
      throw new UnsupportedOperationException("this codec cannot index offsets");
    }
    skipWriter.setIndexOptions(indexOptions);
    lastSkipFP = 0;
    lastState = setEmptyState();
    return 0;
  }

  private Siren10TermState setEmptyState() {
    Siren10TermState emptyState = new Siren10TermState();
    emptyState.docIndex = docOut.index();
    emptyState.skipFP = 0;
    return emptyState;
  }

  /**
   * Adds a new doc in this term. If this returns null
   * then we just skip consuming positions.
   * <p>
   * {@code termDocFreq} parameter is ignored as term frequency in document is
   * not used.
   */
  @Override
  public void startDoc(final int docID, final int termDocFreq)
  throws IOException {
    if (docID < 0) {
      throw new CorruptIndexException("docs out of order (" + docID + ") (docOut: " + docOut + ")");
    }

    if (docWriter.isFull()) {
      if ((++blockCount % blockSkipInterval) == 0) {
        skipWriter.setSkipData(docWriter.getFirstDocId());
        skipWriter.bufferSkip(blockCount);
      }
      docWriter.flush();
      nodWriter.flush(); // flush node block to synchronise it with doc block
      posWriter.flush(); // flush pos block to synchronise it with doc block
    }

    docWriter.write(docID);

    // reset current node for delta computation
    nodWriter.resetCurrentNode();

    // reset payload hash to sentinel value
    lastNodeHash = Long.MAX_VALUE;
  }

  /**
   * Sentinel value {@link Long#MAX_VALUE} is necessary in order to avoid
   * equality with nodes composed of '0' values.
   * <p>
   * Use long to avoid collision between sentinel value and payload hashcode.
   * <p>
   * Using payload hashcode seems to be the fastest way for testing node
   * equality. See micro-benchmark {@link NodeEqualityBenchmark}.
   */
  private long lastNodeHash = Long.MAX_VALUE;

  private final VIntPayloadCodec sirenPayload = new VIntPayloadCodec();

  private int nodeFreqInDoc = 0;
  private int termFreqInNode = 0;

  @Override
  public void addPosition(final int position, final BytesRef payload,
                          final int startOffset, final int endOffset)
  throws IOException {
    assert indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    // we always receive node ids in the payload
    assert payload != null;

    // decode payload
    sirenPayload.decode(payload);
    final IntsRef node = sirenPayload.getNode();

    // check if we received the same node
    // TODO: we pay the cost of decoding the node before testing the equality
    // we could instead directly compute the node hash based on the byte array
    final int nodeHash = node.hashCode();
    if (lastNodeHash != nodeHash) { // if different node
      // add term freq for previous node if not first payload.
      if (lastNodeHash != Long.MAX_VALUE) {
        this.addTermFreqInNode();
      }
      // add new node
      this.addNode(node);
    }
    lastNodeHash = nodeHash;

    // add position
    this.addPosition(sirenPayload.getPosition());
  }

  private void addNode(final IntsRef node) {
    nodWriter.write(node);
    nodeFreqInDoc++;
    // reset current position for delta computation
    posWriter.resetCurrentPosition();
  }

  private void addPosition(final int position) {
    posWriter.write(position);
    termFreqInNode++;
  }

  private void addNodeFreqInDoc() {
    docWriter.writeNodeFreq(nodeFreqInDoc);
    nodeFreqInDoc = 0;
  }

  private void addTermFreqInNode() {
    nodWriter.writeTermFreq(termFreqInNode);
    termFreqInNode = 0;
  }

  @Override
  public void finishDoc() {
    this.addNodeFreqInDoc();
    this.addTermFreqInNode();
  }

  private static class Siren10TermState extends BlockTermState {
    public BlockIndexOutput.Index docIndex;
    public long skipFP;
    public int blockCount;
  }

  /**
   * Called when we are done adding docs to this term
   */
  @Override
  public void finishTerm(final BlockTermState _state) throws IOException {
    Siren10TermState state = (Siren10TermState) _state;
    assert state.docFreq > 0;

    // if block flush pending, write last skip data
    if (!docWriter.isEmpty() && (++blockCount % blockSkipInterval) == 0) {
      skipWriter.setSkipData(docWriter.getFirstDocId());
      skipWriter.bufferSkip(blockCount);
    }

    // flush doc block
    docWriter.flush();
    state.docIndex = docOut.index();
    state.docIndex.copyFrom(docIndex, false);

    // flush node block
    nodWriter.flush();

    // flush pos block
    posWriter.flush();

    // Write skip data to the output file
    if (blockCount >= blockSkipMinimum) {
      state.skipFP = skipOut.getFilePointer();
      skipWriter.writeSkip(skipOut);
    }
    else {
      state.skipFP = -1;
    }

    state.blockCount = blockCount;

    // reset block counter
    blockCount = 0;
  }

  @Override
  public void encodeTerm(final long[] longs, final DataOutput out, final FieldInfo fieldInfo,
                         final BlockTermState _state, final boolean absolute)
  throws IOException {
    Siren10TermState state = (Siren10TermState)_state;

    if (absolute) {
      lastSkipFP = 0;
      lastState = state;
    }

    // write block count stat
    // logger.debug("Write blockCount: {}", state.blockCount);
    out.writeVInt(state.blockCount);

    lastState.docIndex.copyFrom(state.docIndex, false);
    lastState.docIndex.write(out, absolute);

    if (state.skipFP != -1) {
      if (absolute) {
        out.writeVLong(state.skipFP);
      } else {
        out.writeVLong(state.skipFP - lastSkipFP);
      }
      lastSkipFP = state.skipFP;
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (docOut != null) {
        docOut.writeFooter();
      }
      if (skipOut != null) {
        CodecUtil.writeFooter(skipOut);
      }
      if (nodOut != null) {
        nodOut.writeFooter();
      }
      if (posOut != null) {
        posOut.writeFooter();
      }
      success = true;
    }
    finally {
      if (success) {
        IOUtils.close(docOut, skipOut, nodOut, posOut);
      }
      else {
        IOUtils.closeWhileHandlingException(docOut, skipOut, nodOut, posOut);
      }
      docOut = null;
      nodOut = null;
      posOut = null;
      skipOut = null;
    }
  }

  private final MappingMultiDocsNodesAndPositionsEnum postingsEnum = new MappingMultiDocsNodesAndPositionsEnum();

  /**
   * Default merge impl: append documents, nodes and positions, mapping around
   * deletes.
   * <p>
   * Bypass the {@link org.apache.lucene.codecs.PostingsConsumer#merge(org.apache.lucene.index.MergeState, org.apache.lucene.index.FieldInfo.IndexOptions, org.apache.lucene.index.DocsEnum, org.apache.lucene.util.FixedBitSet)}
   * methods and work directly with the BlockWriters for maximum efficiency.
   * <p>
   * TODO - Optimisation: If document blocks match the block size, and no
   * document deleted, then it would be possible to copy block directly as byte
   * array, avoiding decoding and encoding.
   **/
  @Override
  public TermStats merge(final MergeState mergeState, final IndexOptions indexOptions,
                         final DocsEnum postings, final FixedBitSet visitedDocs)
  throws IOException {
    int df = 0;
    long totTF = 0;

    postingsEnum.setMergeState(mergeState);
    postingsEnum.reset((MappingMultiDocsAndPositionsEnum) postings);

    while (postingsEnum.nextDocument()) {
      final int doc = postingsEnum.doc();
      visitedDocs.set(doc);

      this.startDoc(doc, -1);

      final int nodeFreq = postingsEnum.nodeFreqInDoc();
      docWriter.writeNodeFreq(nodeFreq);

      while (postingsEnum.nextNode()) {
        final IntsRef node = postingsEnum.node();
        nodWriter.write(node);

        final int termFreqInNode = postingsEnum.termFreqInNode();
        nodWriter.writeTermFreq(termFreqInNode);

        // reset current position for delta computation
        posWriter.resetCurrentPosition();

        while (postingsEnum.nextPosition()) {
          final int position = postingsEnum.pos();
          posWriter.write(position);
          totTF++;
        }
      }
      df++;
    }

    return new TermStats(df, totTF);
  }

}
