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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.PositionsIterator;
import com.sindicetech.siren.search.node.NodeScorer;
import com.sindicetech.siren.util.ArrayUtils;
import com.sindicetech.siren.util.NodeUtils;

import java.io.IOException;

/**
 * An enumeration of spans based on node positions.
 * <p>
 * The method {@link Spans#node()} returns the parent node of the matching spans. The contract with the
 * {@link org.apache.lucene.util.IntsRef} object that is returned is as follows:
 * <ul>
 *   <li>a slice representing a node identifier</li>
 *   <li>a slice with <code>length == 0</code> to indicate that the parent node is the document root (level 0)</li>
 *   <li>{@link com.sindicetech.siren.index.DocsAndNodesIterator#NULL_NODE} if the enumeration of nodes has not yet started.</li>
 *   <li>{@link com.sindicetech.siren.index.DocsAndNodesIterator#NO_MORE_NOD} if the enumeration of nodes is exhausted.</li>
 * </ul>
 * <p>
 * Code taken from {@link org.apache.lucene.search.spans.TermSpans} and adapted for the Siren use case.
 */
class NodeSpans extends Spans {

  /**
   * The scorer that defines the node spans
   */
  protected final NodeScorer scorer;

  /**
   * Reusable reference for the current node
   */
  private final IntsRef currentNode = new IntsRef();

  /**
   * Reusable reference for the parent of the current node
   */
  private final IntsRef parentCurrentNode = new IntsRef();

  /**
   * Reusable reference for the previous node
   */
  private final IntsRef previousNode = new IntsRef();

  /**
   * Reusable reference for the parent of the previous node
   */
  private final IntsRef parentPreviousNode = new IntsRef();

  /**
   * The start position of the matching span
   */
  protected int matchStart = -1;

  /**
   * true before first call to {@link #nextPosition()}
   */
  private boolean pendingPosition;

  /**
   * true if the current node is pending
   */
  private boolean pendingNode;

  public NodeSpans(final NodeScorer scorer) {
    this.scorer = scorer;
  }

  // only for EmptyNodeSpans (below)
  NodeSpans() {
    scorer = null;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    // reset currentNode and matchStart
    this.resetCurrentNode();
    this.resetStart();

    if (!scorer.nextCandidateDocument()) {
      return false;
    }

    this.pendingNode = false;
    return true;
  }

  @Override
  public boolean nextNode() throws IOException {
    // reset matchStart
    this.resetStart();
    // reset flag for pending position
    pendingPosition = true;

    if (pendingNode) {
      pendingNode = false;
      return true;
    }
    else {
      return this.toNextParent();
    }
  }

  /**
   * Move to the next parent
   */
  private boolean toNextParent() throws IOException {
    // update previous so that previous = current at the start of the loop
    this.updatePreviousNode();

    // move to the next node while parents are equal
    while (NodeUtils.compare(parentCurrentNode, parentPreviousNode) == 0) {
      if (!scorer.nextNode()) {
        this.updatePreviousNode();
        this.updateCurrentNode();
        return false;
      }

      this.updatePreviousNode();
      this.updateCurrentNode();
    }

    return true;
  }

  private void updatePreviousNode() {
    // set current to previous
    ArrayUtils.shallowCopy(currentNode, previousNode);

    // update the parents
    this.setParent(previousNode, parentPreviousNode);
  }

  private void updateCurrentNode() {
    // update the current node
    ArrayUtils.shallowCopy(scorer.node(), currentNode);

    // update the parents
    this.setParent(currentNode, parentCurrentNode);
  }

  @Override
  public boolean nextPosition() throws IOException {
    // the first time we call, we are already positioned on the first child node
    if (pendingPosition) {
      pendingPosition = false;
      matchStart = currentNode.ints[currentNode.offset + currentNode.length - 1];
      return true;
    }
    // otherwise, move to the next node and check if it belongs to the same parent
    else {
      if (!scorer.nextNode()) {
        // do not update previous and current node, as we do not want to loose the current matching node
        // there will be updated next time nextNode is called.
        matchStart = PositionsIterator.NO_MORE_POS;
        return false;
      }

      this.updatePreviousNode();
      this.updateCurrentNode();

      // if the next parent node is different than the current parent node, then we have exhausted the position
      if (NodeUtils.compare(parentCurrentNode, parentPreviousNode) != 0) {
        matchStart = PositionsIterator.NO_MORE_POS;
        pendingNode = true;
        return false;
      }

      // update the span start
      matchStart = currentNode.ints[currentNode.offset + currentNode.length - 1];

      pendingNode = false;

      return true;
    }
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    // reset currentNode and matchStart
    this.resetCurrentNode();
    this.resetStart();

    if (!scorer.skipToCandidate(target)) {
      return false;
    }

    this.pendingNode = false;
    return true;
  }

  @Override
  public int doc() {
    return scorer.doc();
  }

  @Override
  public IntsRef node() {
    return parentCurrentNode;
  }

  /**
   * Create a parent node representation from the source node.
   * <p>
   * If the source node is a sentinel value, i.e., {@link DocsAndNodesIterator#NULL_NODE} or
   * {@link DocsAndNodesIterator#NO_MORE_NOD}, the parent node will be a sentinel value.
   */
  private void setParent(IntsRef sourceNode, IntsRef parentNode) {
    ArrayUtils.shallowCopy(sourceNode, parentNode);
    if ((NodeUtils.compare(parentNode, DocsAndNodesIterator.NULL_NODE) == 0) ||
        (NodeUtils.compare(parentNode, DocsAndNodesIterator.NO_MORE_NOD) == 0)) {
      return;
    }
    parentNode.length = parentNode.length > 0 ? parentNode.length - 1 : 0;
  }

  @Override
  public int start() {
    return matchStart;
  }

  @Override
  public int end() {
    return matchStart + 1;
  }

  @Override
  public float scoreInNode() throws IOException {
    return scorer.scoreInNode();
  }

  @Override
  public int getSlop() {
    return 0;
  }

  /**
   * Reset the {@link #currentNode} to {@link DocsAndNodesIterator#NULL_NODE}
   */
  private void resetCurrentNode() {
    ArrayUtils.shallowCopy(DocsAndNodesIterator.NULL_NODE, currentNode);
    this.setParent(currentNode, parentCurrentNode);
  }

  /**
   * Reset the {@link #matchStart} to sentinel value -1
   */
  private void resetStart() {
    this.matchStart = -1;
  }

  @Override
  public String toString() {
    int doc = this.doc();
    IntsRef node = this.node();
    int position = this.start();

    return "spans(" + scorer.toString() + ")@" +
            (doc == -1 ? "START" : (doc == DocsAndNodesIterator.NO_MORE_DOC) ? "END" : doc + "-" + node + "-" + position);
  }

  private static final class EmptyNodeSpans extends NodeSpans {

    @Override
    public boolean nextCandidateDocument() throws IOException {
      return false;
    }

    @Override
    public boolean nextNode() throws IOException {
      return false;
    }

    @Override
    public boolean nextPosition() throws IOException {
      return false;
    }

    @Override
    public boolean skipToCandidate(final int target) throws IOException {
      return false;
    }

    @Override
    public int doc() {
      return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public IntsRef node() {
      return DocsAndNodesIterator.NO_MORE_NOD;
    }

    @Override
    public int start() {
      return PositionsIterator.NO_MORE_POS;
    }

    @Override
    public int end() {
      return PositionsIterator.NO_MORE_POS;
    }

    @Override
    public float scoreInNode() {
      return 0;
    }

    @Override
    public int getSlop() {
      return 0;
    }

  }

  public static final NodeSpans EMPTY_NODE_SPANS = new EmptyNodeSpans();

}
