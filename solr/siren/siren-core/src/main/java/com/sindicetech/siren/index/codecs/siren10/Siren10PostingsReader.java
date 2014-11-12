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

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.index.DocsNodesAndPositionsEnum;
import com.sindicetech.siren.index.SirenDocsEnum;
import com.sindicetech.siren.index.codecs.block.BlockIndexInput;
import com.sindicetech.siren.search.node.NodeScorer;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Reads the document identifiers, node frequencies, node labels, term
 * frequencies, term positions and block skip data.
 */
public class Siren10PostingsReader extends PostingsReaderBase {

  final DocsFreqBlockIndexInput docIn;
  final NodBlockIndexInput nodIn;
  final PosBlockIndexInput posIn;

  final IndexInput skipIn;

  int blockSkipInterval;
  int maxSkipLevels;
  int blockSkipMinimum;
  int maxBlockSize;

  protected static final Logger logger = LoggerFactory.getLogger(Siren10PostingsReader.class);

  public Siren10PostingsReader(final Directory dir, final SegmentInfo segmentInfo,
                               final IOContext context, final String segmentSuffix,
                               final Siren10BlockStreamFactory factory)
  throws IOException {
    boolean success = false;
    try {
      final String docFileName = IndexFileNames.segmentFileName(segmentInfo.name,
        segmentSuffix, Siren10PostingsFormat.DOC_EXTENSION);
      docIn = factory.openDocsFreqInput(dir, docFileName, context);

      nodIn = factory.openNodInput(dir, IndexFileNames.segmentFileName(segmentInfo.name,
        segmentSuffix, Siren10PostingsFormat.NOD_EXTENSION), context);

      skipIn = dir.openInput(IndexFileNames.segmentFileName(segmentInfo.name,
        segmentSuffix, Siren10PostingsFormat.SKIP_EXTENSION), context);

      posIn = factory.openPosInput(dir, IndexFileNames.segmentFileName(segmentInfo.name,
        segmentSuffix, Siren10PostingsFormat.POS_EXTENSION), context);

      success = true;
    }
    finally {
      if (!success) {
        this.close();
      }
    }
  }

  @Override
  public void init(final IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching past writer
    CodecUtil.checkHeader(termsIn, Siren10PostingsWriter.CODEC,
      Siren10PostingsWriter.VERSION_START, Siren10PostingsWriter.VERSION_CURRENT);
    blockSkipInterval = termsIn.readInt();
    maxSkipLevels = termsIn.readInt();
    blockSkipMinimum = termsIn.readInt();
    maxBlockSize = termsIn.readInt();
  }

  @Override
  public void close() throws IOException {
    try {
      if (nodIn != null)
        nodIn.close();
    } finally {
      try {
        if (docIn != null)
          docIn.close();
      } finally {
        try {
          if (skipIn != null)
            skipIn.close();
        } finally {
          if (posIn != null) {
            posIn.close();
          }
        }
      }
    }
  }

  private static final class Siren10TermState extends BlockTermState {
    // We store only the seek point to the docs file because
    // the rest of the info (freqIndex, posIndex, etc.) is
    // stored in the docs file:
    BlockIndexInput.Index docIndex;

    long skipFP;
    int blockCount;

    // Only used for "primary" term state; these are never
    // copied on clone:

    @Override
    public Siren10TermState clone() {
      final Siren10TermState other = new Siren10TermState();
      other.copyFrom(this);
      return other;
    }

    @Override
    public void copyFrom(final TermState _other) {
      super.copyFrom(_other);
      final Siren10TermState other = (Siren10TermState) _other;

      blockCount = other.blockCount;

      if (docIndex == null) {
        docIndex = (BlockIndexInput.Index) other.docIndex.clone();
      }
      else {
        docIndex.set(other.docIndex);
      }

      skipFP = other.skipFP;
    }

    @Override
    public String toString() {
      return super.toString() + " docIndex=" + docIndex + " skipFP=" + skipFP
        + " blockCount=" + blockCount;
    }
  }

  @Override
  public BlockTermState newTermState() throws IOException {
    final Siren10TermState state = new Siren10TermState();
    state.docIndex = docIn.index();
    return state;
  }

  @Override
  public void decodeTerm(long[] empty, DataInput in, FieldInfo fieldInfo, BlockTermState _termState, boolean absolute)
  throws IOException {
    final Siren10TermState termState = (Siren10TermState) _termState;

    termState.blockCount = in.readVInt();

    termState.docIndex.read(in, absolute);

    if (termState.blockCount >= blockSkipMinimum) {
      if (absolute) {
        termState.skipFP = in.readVLong();
      }
      else {
        termState.skipFP += in.readVLong();
      }
    }
    else if (absolute) {
      termState.skipFP = 0;
    }
  }

  @Override
  public DocsEnum docs(final FieldInfo fieldInfo, final BlockTermState termState,
                       final Bits liveDocs, final DocsEnum reuse,
                       final int flags) throws IOException {
    Siren10DocsEnum docsEnum;

    if (this.canReuse(reuse, liveDocs)) {
      docsEnum = (Siren10DocsEnum) reuse;
    }
    else {
      docsEnum = new Siren10DocsEnum();
    }
    return docsEnum.init(fieldInfo, (Siren10TermState) termState, liveDocs);
  }

  private boolean canReuse(final DocsEnum reuse, final Bits liveDocs) {
    if (reuse != null && (reuse instanceof Siren10DocsEnum)) {
      final Siren10DocsEnum docsEnum = (Siren10DocsEnum) reuse;
      // If you are using ParallelReader, and pass in a
      // reused DocsEnum, it could have come from another
      // reader also using standard codec
      if (docsEnum.getDocsNodesAndPositionsEnum().startDocIn == docIn) {
        // we only reuse if the the actual the incoming enum has the same liveDocs as the given liveDocs
        return liveDocs == docsEnum.getDocsNodesAndPositionsEnum().liveDocs;
      }
    }
    return false;
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(final FieldInfo fieldInfo,
                                               final BlockTermState termState,
                                               final Bits liveDocs,
                                               final DocsAndPositionsEnum reuse,
                                               final int flags)
  throws IOException {
    return (DocsAndPositionsEnum) this.docs(fieldInfo, termState, liveDocs, reuse, flags);
  }

  /**
   * This {@link DocsAndPositionsEnum} implementation is a decorator over a
   * {@link DocsNodesAndPositionsEnum} which:
   * <ul>
   * <li> is used to supply the {@link DocsNodesAndPositionsEnum} in
   * {@link Siren10PostingsWriter#merge(org.apache.lucene.index.MergeState,
   * org.apache.lucene.index.FieldInfo.IndexOptions,
   * org.apache.lucene.index.DocsEnum, org.apache.lucene.util.FixedBitSet)}
   * and in {@link NodeScorer}.
   * <li> emulate a {@link DocsAndPositionsEnum} to be compatible with Lucene's
   * internal mechanism, especially with {@link CheckIndex}.
   * </ul>
   * <p>
   * This implementation is very inefficient and should not be used outside
   * unit tests.
   * <p>
   * Positions in {@link DocsNodesAndPositionsEnum} are local to a node.
   * This implementation emulates {@link #nextPosition()} by scaling
   * up positions with a position gap that are relative to the node id.
   * Therefore, the positions returned by this enum are not the real positions.
   * <p>
   * The position gap is based on a hash of the node id. The hash of the
   * node id is computed by normalising the node order between 0 and
   * {@link Integer#MAX_VALUE}.
   * <p>
   * If this enum is used with Lucene's Positional Scorers, there is a chance
   * of false-positive results.
   * <p>
   * There is a chance that the position returned is negative, in case the
   * number of nodes are close to {@link Integer#MAX_VALUE}.
   * <p>
   * This position is only used in {@link CheckIndex} after unit tests, where
   * the node tree structure is relatively simple.
   */
  class Siren10DocsEnum extends SirenDocsEnum {

    private final Siren10DocsNodesAndPositionsEnum docEnum;

    private final LinkedList<Integer> positions = new LinkedList<Integer>();

    Siren10DocsEnum() throws IOException {
      docEnum = new Siren10DocsNodesAndPositionsEnum();
    }

    Siren10DocsEnum init(final FieldInfo fieldInfo, final Siren10TermState termState, final Bits liveDocs)
    throws IOException {
      docEnum.init(fieldInfo, termState, liveDocs);
      return this;
    }

    @Override
    public Siren10DocsNodesAndPositionsEnum getDocsNodesAndPositionsEnum() {
      return docEnum;
    }

    @Override
    public int nextDoc() throws IOException {
      docEnum.nextDocument();
      return docEnum.doc();
    }

    @Override
    public int freq() throws IOException {
      // clear position cache
      positions.clear();
      // compute increment for scaling up node hash
      final int inc = Integer.MAX_VALUE / (docEnum.nodeFreqInDoc() + 1);

      int freq = 0;
      int hash = 0;

      while (docEnum.nextNode()) {
        // scale up hash node based on hash increment
        hash += inc;

        while (docEnum.nextPosition()) {
          freq++;
          // cache position
          positions.add(hash + docEnum.pos());
        }

      }
      return freq;
    }

    @Override
    public int docID() {
      return docEnum.doc();
    }

    @Override
    public int advance(final int target) throws IOException {
      docEnum.skipTo(target);
      return docEnum.doc();
    }

    @Override
    public int nextPosition() throws IOException {
      return positions.poll();
    }

    @Override
    public int startOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public long cost() {
      return docEnum.docLimit;
    }

  }

  /**
   * Implementation of {@link DocsNodesAndPositionsEnum} for the SIREn 1.0
   * postings format.
   *
   * <p>
   *
   * Relies on lazy-loading of the {@link BlockIndexInput}s as much as possible.
   */
  class Siren10DocsNodesAndPositionsEnum extends DocsNodesAndPositionsEnum {

    int docLimit;
    int blockLimit;

    int doc = -1;
    int docCount;
    int nodFreq = 0;
    int termFreqInNode = 0;
    IntsRef node = new IntsRef(new int[] { -1 }, 0, 1);
    int pos = -1;

    // flag to know if nextNode() has been called
    boolean termFreqInNodeReadPending = false;

    private int pendingNodFreqCount;
    private int pendingNodCount;
    private int pendingTermFreqInNodeCount;
    private int pendingPosNodCount;

    private Bits liveDocs;
    private final DocsFreqBlockIndexInput.DocsFreqBlockReader docReader;
    private final NodBlockIndexInput.NodBlockReader nodReader;
    private final PosBlockIndexInput.PosBlockReader posReader;
    private long skipFP;

    private final BlockIndexInput.Index docIndex;
    private final BlockIndexInput.Index nodIndex;
    private final BlockIndexInput.Index posIndex;
    private final DocsFreqBlockIndexInput startDocIn;

    boolean skipped;
    Siren10SkipListReader skipper;

    Siren10DocsNodesAndPositionsEnum() throws IOException {
      startDocIn = docIn;

      docReader = docIn.getBlockReader();
      docIndex = docIn.index();

      nodReader = nodIn.getBlockReader();
      nodIndex = nodIn.index();

      posReader = posIn.getBlockReader();
      posIndex = posIn.index();

      // register node and pos index in the doc reader
      docReader.setNodeBlockIndex(nodIndex);
      docReader.setPosBlockIndex(posIndex);
    }

    Siren10DocsNodesAndPositionsEnum init(final FieldInfo fieldInfo,
                                          final Siren10TermState termState,
                                          final Bits liveDocs)
    throws IOException {
      // logger.debug("Init DocsNodesAndPositionsEnum - id={}", this.hashCode());
      this.liveDocs = liveDocs;

      // Init readers
      docReader.init();
      nodReader.init();
      posReader.init();

      // TODO: can't we only do this if consumer
      // skipped consuming the previous docs?
      // logger.debug("Set docIndex: {}", termState.docIndex);
      docIndex.set(termState.docIndex);
      docIndex.seek(docReader);

      docLimit = termState.docFreq;
      blockLimit = termState.blockCount;

      // NOTE: unused if blockCount < skipMinimum:
      skipFP = termState.skipFP;

      doc = -1;
      this.resetFreqNodAndPos();

      docCount = 0;

      this.resetPendingCounters();

      skipped = false;

      return this;
    }

    private void resetPendingCounters() {
      pendingNodFreqCount = 0;
      pendingNodCount = 0;
      pendingTermFreqInNodeCount = 0;
      pendingPosNodCount = 0;
      termFreqInNodeReadPending = false;
    }

    /**
     * TODO: is it needed ?
     */
    private final IntsRef UNSET_NODE = new IntsRef(new int[] { -1 }, 0, 1);

    /**
     * Reset the freqs to 0 and the current node and position to -1.
     */
    private void resetFreqNodAndPos() {
      nodFreq = termFreqInNode = 0; // lazy load of freq
      node = UNSET_NODE;
      pos = -1;
    }

    @Override
    public boolean nextDocument() throws IOException {
      do {
        if (docCount == docLimit) {
          doc = NO_MORE_DOC;
          node = NO_MORE_NOD;
          pos = NO_MORE_POS;
          // to stop reading and decoding data in #nextNode and #nextPosition
          this.resetPendingCounters();
          return false;
        }

        docCount++;

        // If block exhausted, decode next block
        if (docReader.isExhausted()) {
          docReader.nextBlock();
          nodIndex.seek(nodReader); // move node reader to next block
          nodReader.nextBlock(); // doc and node blocks are synchronised
          posIndex.seek(posReader); // move node reader to next block
          posReader.nextBlock(); // doc and pos blocks are synchronised
          this.resetPendingCounters(); // reset counters as we move to next block
        }
        // decode next doc
        doc = docReader.nextDocument();
        this.resetFreqNodAndPos(); // reset freqs, node and pos
        termFreqInNodeReadPending = false; // reset flag
        // increment node freq pending counters
        pendingNodFreqCount++;
      } while (liveDocs != null && !liveDocs.get(doc));

      return true;
    }

    @Override
    public boolean nextNode() throws IOException {
      termFreqInNode = 0; // lazy load of freq
      termFreqInNodeReadPending = true;
      pos = -1; // reset position
      final int nodeFreqInDoc = this.nodeFreqInDoc(); // load node freq

      if (pendingNodCount > 0) {
        if (pendingNodCount > nodeFreqInDoc) {
          nodReader.skipNodes(pendingNodCount - nodeFreqInDoc);
          pendingNodCount = nodeFreqInDoc;
        }

        if (pendingNodCount == nodeFreqInDoc) { // start of the new doc
          // reset current node for delta computation
          nodReader.resetCurrentNode();
        }
        // no need to check for exhaustion as doc and node blocks are synchronised
        node = nodReader.nextNode();
        pendingNodCount--;
        assert pendingNodCount >= 0;
        return true;
      }
      assert pendingNodCount == 0;
      node = NO_MORE_NOD; // set to sentinel value
      return false;
    }

    @Override
    public boolean skipTo(final int target) throws IOException {
      if ((target - (blockSkipInterval * maxBlockSize)) >= doc &&
          docLimit >= (blockSkipMinimum * maxBlockSize)) {

        // There are enough docs in the posting to have
        // skip data, and its not too close

        if (skipper == null) {
          // This DocsEnum has never done any skipping
          skipper = new Siren10SkipListReader(skipIn.clone(),
                                              docIn, maxSkipLevels,
                                              blockSkipInterval, maxBlockSize);
        }

        if (!skipped) {
          // We haven't yet skipped for this posting
          skipper.init(skipFP, docIndex, blockLimit);
          skipper.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
          skipped = true;
        }
        final int newCount = skipper.skipTo(target);

        if (newCount > docCount) {
          // Skipper did move
          skipper.getDocIndex().seek(docReader);
          docCount = newCount;
          doc = skipper.getDoc();
          // reset so that it is consider exhausted in #nextDocument and move
          // to the next block
          docReader.initBlock();
        }
      }

      // Now, linear scan for the rest:
      // TODO: Implement linear block skipping based on first and last doc ids
      do {
        if (!this.nextDocument()) {
          return false;
        }
      } while (target > doc);

      return true;
    }

    @Override
    public int doc() {
      return doc;
    }

    @Override
    public IntsRef node() {
      return node;
    }

    @Override
    public boolean nextPosition() throws IOException {
      final int termFreqInNode = this.termFreqInNode(); // load term freq
      // scan over any positions that were ignored during doc iteration
      while (pendingPosNodCount > termFreqInNode) {
        // no need to check for exhaustion as doc and pos blocks are synchronised
        pos = posReader.nextPosition();
        pendingPosNodCount--;
      }

      assert pendingPosNodCount <= termFreqInNode;

      if (pendingPosNodCount > 0) {
        if (pendingPosNodCount == termFreqInNode) { // start of the new node
          // reset current position for delta computation
          posReader.resetCurrentPosition();
        }
        // no need to check for exhaustion as doc and pos blocks are synchronised
        pos = posReader.nextPosition();
        pendingPosNodCount--;
        assert pendingPosNodCount >= 0;
        return true;
      }
      assert pendingPosNodCount == 0;
      pos = NO_MORE_POS; // set to sentinel value
      return false;
    }

    @Override
    public int pos() {
      return pos;
    }

    @Override
    public int nodeFreqInDoc() throws IOException {
      if (nodFreq == 0) {
        // scan over any freqs that were ignored during doc iteration
        while (pendingNodFreqCount > 0) {
          nodFreq = docReader.nextNodeFreq();
          pendingNodFreqCount--;
          pendingNodCount += nodFreq;
          pendingTermFreqInNodeCount += nodFreq;
        }
      }
      return nodFreq;
    }

    @Override
    public int termFreqInNode() throws IOException {
      // nextNode should be called first
      if (termFreqInNodeReadPending) {
        // scan over any freqs that were ignored during doc and node iteration
        // we need to account for the last call to nextNode which decrements pendingNodCount, hence + 1
        while (pendingTermFreqInNodeCount > pendingNodCount + 1) {
          termFreqInNode = nodReader.nextTermFreqInNode();
          pendingTermFreqInNodeCount--;
          pendingPosNodCount += termFreqInNode;
        }

        // scan next freq
        termFreqInNode = nodReader.nextTermFreqInNode();
        pendingTermFreqInNodeCount--;
        pendingPosNodCount += termFreqInNode;

        // reset flag
        termFreqInNodeReadPending = false;
      }
      return termFreqInNode;
    }

  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  @Override
  public void checkIntegrity() throws IOException {
    if (docIn != null) {
      docIn.checksumEntireFile();
    }
    if (nodIn != null) {
      nodIn.checksumEntireFile();
    }
    if (posIn != null) {
      posIn.checksumEntireFile();
    }
    if (skipIn != null) {
      CodecUtil.checksumEntireFile(skipIn);
    }
  }

}

