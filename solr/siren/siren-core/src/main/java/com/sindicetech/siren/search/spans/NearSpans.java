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
package com.sindicetech.siren.search.spans;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.util.NodeUtils;

import java.io.IOException;
import java.util.List;

/**
 * A spans that is formed from the sub-spans of a NearSpanQuery
 * where the sub-spans have a maximum slop between them.
 * <p>
 * The score is computed as the sum of the sub-spans' score weighted by the sloppy weight.
 */
abstract class NearSpans extends Spans {

  protected final int allowedSlop;

  /**
   * The spans in the same order as the NearSpanQuery
   */
  protected final Spans[] subSpans;

  /**
   * The spans array that will be used for the sorting. This is necessary as we do not want to lose the order of
   * spans in {@link #subSpans}.
   */
  private final Spans[] subSpansSort;

  private int matchDoc = -1;
  private IntsRef matchNode = DocsAndNodesIterator.NULL_NODE;
  protected int matchStart = -1;
  protected int matchEnd = -1;

  // Even though the array is probably almost sorted, InPlaceMergeSorter will likely
  // perform better since it has a lower overhead than TimSorter for small arrays
  // TODO: Investigate if a priority queue will not be more performant than multiple sorters

  /**
   * A spans array sorter based on the document identifier
   */
  private final InPlaceMergeSorter docSorter = new InPlaceMergeSorter() {
    @Override
    protected void swap(final int i, final int j) {
      ArrayUtil.swap(subSpansSort, i, j);
    }
    @Override
    protected int compare(final int i, final int j) {
      return subSpansSort[i].doc() - subSpansSort[j].doc();
    }
  };

  /**
   * A spans array sorter based on the node
   */
  private final InPlaceMergeSorter nodeSorter = new InPlaceMergeSorter() {
    @Override
    protected void swap(final int i, final int j) {
      ArrayUtil.swap(subSpansSort, i, j);
    }
    @Override
    protected int compare(int i, int j) {
      return NodeUtils.compare(subSpansSort[i].node(), subSpansSort[j].node());
    }
  };

  public NearSpans(final List<Spans> spans, final int slop) throws IOException {
    if (spans.size() < 2) {
      throw new IllegalArgumentException("Less than 2 spans clauses");
    }

    allowedSlop = slop;

    subSpans = new Spans[spans.size()];
    subSpansSort = new Spans[spans.size()];
    for (int i = 0; i < spans.size(); i++) {
      subSpans[i] = spans.get(i);
      subSpansSort[i] = subSpans[i]; // used in toSameDoc()
    }
  }

  @Override
  public int doc() { return matchDoc; }

  @Override
  public IntsRef node() { return matchNode; }

  @Override
  public int start() { return matchStart; }

  @Override
  public int end() { return matchEnd; }

  public Spans[] getSubSpans() {
    return subSpans;
  }

  @Override
  public long cost() {
    long minCost = Long.MAX_VALUE;
    for (int i = 0; i < subSpans.length; i++) {
      minCost = Math.min(minCost, subSpans[i].cost());
    }
    return minCost;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    // reset matchNode, matchStart and matchEnd
    this.resetMatchNode();
    this.resetStartAndEnd();

    // Advance all the spans to their next candidate
    for (int i = 0; i < subSpans.length; i++) {
      if (!subSpans[i].nextCandidateDocument()) {
        matchDoc = DocsAndNodesIterator.NO_MORE_DOC;
        return false;
      }
    }

    // Ensure that all the spans are on the same candidate
    return toSameCandidateDocument();
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    // reset matchNode, matchStart and matchEnd
    this.resetMatchNode();
    this.resetStartAndEnd();

    // Advance all the spans to or after the target
    for (int i = 0; i < subSpans.length; i++) {
      if (!subSpans[i].skipToCandidate(target)) {
        matchDoc = DocsAndNodesIterator.NO_MORE_DOC;
        return false;
      }
    }

    // Ensure that all the spans are on the same candidate
    return toSameCandidateDocument();
  }

  /**
   * Advance the spans to the same candidate document
   */
  private boolean toSameCandidateDocument() throws IOException {
    docSorter.sort(0, subSpansSort.length);
    int firstIndex = 0;
    int maxDoc = subSpansSort[subSpansSort.length - 1].doc();

    while (subSpansSort[firstIndex].doc() != maxDoc) {
      if (!subSpansSort[firstIndex].skipToCandidate(maxDoc)) {
        matchDoc = DocsAndNodesIterator.NO_MORE_DOC;
        return false;
      }
      maxDoc = subSpansSort[firstIndex].doc();
      if (++firstIndex == subSpansSort.length) {
        firstIndex = 0;
      }
    }

    // assert doc - loop will be removed by jit if assert is disabled
    for (int i = 0; i < subSpansSort.length; i++) {
      assert (subSpansSort[i].doc() == maxDoc)
              : " NearSpansOrdered.toSameCandidateDocument() spans " + subSpansSort[0]
              + "\n at doc " + subSpansSort[i].doc()
              + ", but should be at " + maxDoc;
    }

    matchDoc = subSpans[0].doc();
    return true;
  }

  @Override
  public boolean nextNode() throws IOException {
    // reset matchStart and matchEnd
    this.resetStartAndEnd();

    // Advance all the spans to their next node
    for (int i = 0; i < subSpans.length; i++) {
      if (!subSpans[i].nextNode()) {
        matchNode = DocsAndNodesIterator.NO_MORE_NOD;
        return false;
      }
    }

    // Ensure that all the spans are on the same node
    return toSameNode();
  }

  /**
   * Advance the spans to the same node
   */
  private boolean toSameNode() throws IOException {
    nodeSorter.sort(0, subSpansSort.length);
    int firstIndex = 0;
    IntsRef maxNode = subSpansSort[subSpansSort.length - 1].node();

    while (NodeUtils.compare(subSpansSort[firstIndex].node(), maxNode) != 0) {
      // Advance the first span to or after the max node
      do {
        if (!subSpansSort[firstIndex].nextNode()) {
          matchNode = DocsAndNodesIterator.NO_MORE_NOD;
          return false;
        }
      // ensure that we have advanced to or after the max node
      } while (NodeUtils.compare(subSpansSort[firstIndex].node(), maxNode) < 0);

      maxNode = subSpansSort[firstIndex].node();
      if (++firstIndex == subSpansSort.length) {
        firstIndex = 0;
      }
    }

    // assert node - loop will be removed by jit if assert is disabled
    for (int i = 0; i < subSpansSort.length; i++) {
      assert (subSpansSort[i].node().equals(maxNode))
              : " NearSpansOrdered.toSameNode() spans " + subSpansSort[0]
              + "\n at node " + subSpansSort[i].node()
              + ", but should be at " + maxNode;
    }

    matchNode = subSpans[0].node();
    return true;
  }

  @Override
  public float scoreInNode() throws IOException {
    float score = 0.0f;

    // Sum the score of the sub-spans
    for (int i = 0; i < subSpans.length; i++) {
      score += subSpans[i].scoreInNode();
    }

    // compute the sloppy weight
    float sloppyWeight = this.sloppyWeight(this.getSlop());
    return score * sloppyWeight;
  }

  /**
   * Reset the {@link #matchNode} to null
   */
  private void resetMatchNode() {
    this.matchNode = DocsAndNodesIterator.NULL_NODE;
  }

  /**
   * Reset the {@link #matchStart} and {@link #matchEnd} to sentinel value -1
   */
  private void resetStartAndEnd() {
    this.matchStart = -1;
    this.matchEnd = -1;
  }

  /**
   * Check whether two {@link com.sindicetech.siren.search.spans.Spans} in the same document are ordered.
   *
   * @return true iff spans1 starts before spans2
   *              or the spans start at the same position,
   *              and spans1 ends before spans2.
   */
  static final boolean isSpansOrdered(final int start1, final int end1, final int start2, final int end2) {
    return (start1 == start2) ? (end1 < end2) : (start1 < start2);
  }

  @Override
  public String toString() {
    int doc = this.doc();
    IntsRef node = this.node();
    int start = this.start();
    int end = this.end();

    return getClass().getName() + "@" +
      (doc == -1 ? "START" : (doc == DocsAndNodesIterator.NO_MORE_DOC) ? "END" : doc + "-" + node + "-" + start + "-" + end);
  }

}

