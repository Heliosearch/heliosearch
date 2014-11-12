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

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.PriorityQueue;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.PositionsIterator;
import com.sindicetech.siren.util.NodeUtils;

import java.io.IOException;
import java.util.List;

/**
 * A disjunction of {@link com.sindicetech.siren.search.spans.Spans} used by {@link OrSpanQuery}.
 */
class OrSpans extends Spans {

  private SpanQueue queue;

  private Spans[] subSpans;

  private boolean qInitialized;

  OrSpans(final List<Spans> spans) {
    this.subSpans = spans.toArray(new Spans[spans.size()]);
    this.queue = new SpanQueue(subSpans.length);
  }

  private boolean initSpanQueue(final int target) throws IOException {
    for (Spans subSpan : subSpans) {
      if (((target == -1) && subSpan.nextCandidateDocument()) || ((target != -1) && subSpan.skipToCandidate(target))) {
        queue.add(subSpan);
      }
    }
    return queue.size() != 0;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    if (!qInitialized) {
      qInitialized = true;
      return initSpanQueue(-1);
    }

    if (queue.size() == 0) { // all done
      return false;
    }

    if (top().nextCandidateDocument()) { // move to next
      queue.updateTop();
      return true;
    }

    queue.pop();  // exhausted a clause
    return queue.size() != 0;
  }

  @Override
  public boolean nextNode() throws IOException {
    return queue.nextNodeAndAdjust();
  }

  @Override
  public boolean nextPosition() throws IOException {
    return queue.nextPositionAndAdjust();
  }

  @Override
  public boolean skipToCandidate(int target) throws IOException {
    if (!qInitialized) {
      qInitialized = true;
      return initSpanQueue(target);
    }

    boolean skipCalled = false;
    while (queue.size() != 0 && top().doc() < target) {
      if (top().skipToCandidate(target)) {
        queue.updateTop();
      } else {
        queue.pop();
      }
      skipCalled = true;
    }

    if (skipCalled) {
      return queue.size() != 0;
    }
    return nextCandidateDocument();
  }

  @Override
  public float scoreInNode() throws IOException {
    return queue.scoreInNode();
  }

  @Override
  public int getSlop() {
    return top().getSlop();
  }

  @Override
  public int doc() {
    if (top() == null) {
      return -1;
    }
    return top().doc();
  }

  @Override
  public IntsRef node() {
    return top().node();
  }

  @Override
  public int start() {
    return top().start();
  }

  @Override
  public int end() {
    return top().end();
  }

  private Spans top() {
    return queue.top();
  }

  /**
   * A {@link org.apache.lucene.util.PriorityQueue} of {@link com.sindicetech.siren.search.spans.TermSpans} which provides
   * helper methods to advance the least spans to their next node or position, and to sum the score of the least
   * spans.
   * <p>
   * TODO: Optimisation
   * Many code duplicate in {@link #countNodeEqualToTop(int)}, {@link #countPositionEqualToTop(int)} and
   * {@link #sumScoreInNode(int)}. In addition, this also duplicates computation. For example, we compare the same nodes
   * three times, in each of the methods. A better implementation could reuse such a computation. A possible approach
   * would be to use a bitset to track which spans in the queue matches or not. From the bitset, we can quickly get a
   * count, or iterate over the index of the queue array.
   */
  private class SpanQueue extends PriorityQueue<Spans> {

    public SpanQueue(int size) {
      super(size);
    }

    @Override
    protected final boolean lessThan(Spans spans1, Spans spans2) {
      if (spans1.doc() == spans2.doc()) {
        int comparison = NodeUtils.compare(spans1.node(), spans2.node());
        if (comparison == 0) {
          if (spans1.start() == spans2.start()) {
            return spans1.end() < spans2.end();
          }
          else {
            return spans1.start() < spans2.start();
          }
        }
        else {
          return comparison < 0;
        }
      }
      else {
        return spans1.doc() < spans2.doc();
      }
    }

    /**
     * Perform a traversal of the heap binary tree using recursion. Given a node,
     * visit its children and check if their spans is equivalent to the least
     * spans. If the spans is equivalent, it increments the count and recursively visit its
     * two children.
     */
    private final int countNodeEqualToTop(final int root) throws IOException {
      int count = 0;

      final int i1 = (root << 1); // index of first child node
      final int i2 = i1 + 1; // index of second child node

      Spans top = this.top();

      if (i1 <= this.size()) {
        final Spans child1 = (Spans) this.getHeapArray()[i1];
        if (nodeEqual(top, child1)) {
          count++;
          count += this.countNodeEqualToTop(i1);
        }
      }

      if (i2 <= this.size()) {
        final Spans child2 = (Spans) this.getHeapArray()[i2];
        if (nodeEqual(top, child2)) {
          count++;
          count += this.countNodeEqualToTop(i2);
        }
      }

      return count;
    }

    /**
     * Move all the least spans in the queue that are positioned on the document and node to their
     * next node and adjust the heap.
     *
     * @return If the least spans has no more nodes, returns false.
     */
    public final boolean nextNodeAndAdjust() throws IOException {
      // Count number of spans, starting from the top, having the same document and node.
      // Add 1 to count the top.
      int count = this.countNodeEqualToTop(1) + 1;

      // Move the spans to the next node
      for (int i = 0; i < count; i++) {
        this.top().nextNode();
        this.updateTop();
      }

      // if the top's position is the sentinel value, it means that the nodes are exhausted
      return (NodeUtils.compare(this.top().node(), DocsAndNodesIterator.NO_MORE_NOD) == 0) ? false : true;
    }

    /**
     * Perform a traversal of the heap binary tree using recursion. Given a node,
     * visit its children and check if their spans is equivalent to the least
     * spans. If the spans is equivalent, it increments the count and recursively visit its
     * two children.
     */
    private final int countPositionEqualToTop(final int root) throws IOException {
      int count = 0;

      final int i1 = (root << 1); // index of first child node
      final int i2 = i1 + 1; // index of second child node

      Spans top = this.top();

      if (i1 <= this.size()) {
        final Spans child1 = (Spans) this.getHeapArray()[i1];
        if (positionEqual(top, child1)) {
          count++;
          count += this.countPositionEqualToTop(i1);
        }
      }

      if (i2 <= this.size()) {
        final Spans child2 = (Spans) this.getHeapArray()[i2];
        if (positionEqual(top, child2)) {
          count++;
          count += this.countPositionEqualToTop(i2);
        }
      }

      return count;
    }

    /**
     * Move all the least spans in the queue that are positioned on the same document, node and position to their
     * next position and adjust the heap.
     *
     * @return If the least spans has no more positions, returns false.
     */
    public final boolean nextPositionAndAdjust() throws IOException {
      // Count number of spans, starting from the top, having the same document, node and position.
      // Add 1 to count the top.
      int count = this.countPositionEqualToTop(1) + 1;

      // Move the spans to the next position
      for (int i = 0; i < count; i++) {
        this.top().nextPosition();
        this.updateTop();
      }

      // if the top's position is the sentinel value, it means that the positions are exhausted
      return (this.top().start() == PositionsIterator.NO_MORE_POS) ? false : true;
    }

    public float scoreInNode() throws IOException {
      // Sum the score of spans, starting from the top, having the same document and node.
      // Add the score of the top.
      return this.sumScoreInNode(1) + this.top().scoreInNode();
    }

    /**
     * Perform a traversal of the heap binary tree using recursion. Given a node,
     * visit its children and check if their spans is equivalent to the least
     * spans. If the spans is equivalent, it sums its score for the current node and recursively visit its
     * two children.
     */
    private final float sumScoreInNode(final int root) throws IOException {
      float score = 0;

      final int i1 = (root << 1); // index of first child node
      final int i2 = i1 + 1; // index of second child node

      Spans top = this.top();

      if (i1 <= this.size()) {
        final Spans child1 = (Spans) this.getHeapArray()[i1];
        if (nodeEqual(top, child1)) {
          score += child1.scoreInNode();
          score += this.sumScoreInNode(i1);
        }
      }

      if (i2 <= this.size()) {
        final Spans child2 = (Spans) this.getHeapArray()[i2];
        if (nodeEqual(top, child2)) {
          score += child2.scoreInNode();
          score += this.sumScoreInNode(i2);
        }
      }

      return score;
    }

    private boolean nodeEqual(Spans spans1, Spans spans2) {
      return spans1.doc() == spans2.doc() && (NodeUtils.compare(spans1.node(), spans2.node()) == 0);
    }

    private boolean positionEqual(Spans spans1, Spans spans2) {
      return spans1.doc() == spans2.doc() && (NodeUtils.compare(spans1.node(), spans2.node()) == 0) &&
              spans1.start() == spans2.start() && spans1.end() == spans2.end();
    }

  }

}