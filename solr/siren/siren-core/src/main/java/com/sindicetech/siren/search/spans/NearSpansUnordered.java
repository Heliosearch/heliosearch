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

import org.apache.lucene.util.PriorityQueue;

import com.sindicetech.siren.index.PositionsIterator;
import com.sindicetech.siren.util.NodeUtils;

import java.io.IOException;
import java.util.List;

/**
 * Similar to {@link NearSpansOrdered}, but for the unordered case.
 * <p>
 * The spans are considered matching when the length between the start position of the minimum spans and the end of the
 * maximum spans, minus the total lengths of the spans, is inferior to the allowed slop. We therefore need to keep
 * track of the min and max to check for match.
 * <p>
 * We keep a partial order of the {@link TermSpans} based on their position with a
 * {@link org.apache.lucene.util.PriorityQueue} in order to find the minimum in constant time. The cost is k * log(n)
 * with k being the number of positions across all spans, and log(n) the cost of keeping the queue partially ordered.
 * <p>
 * We keep track of the max by checking it whenever the position of a {@link TermSpans}
 * is modified.
 */
class NearSpansUnordered extends NearSpans {

  /**
   * sorted queue of spans based on position
   */
  private SpansQueue queue;

  /**
   * max element in queue
   */
  private Spans max;

  /**
   * sum of the lengths of the current spans
   */
  private int totalLength;

  /**
   * true before first call to {@link #nextPosition()}
   */
  private boolean firstTime;

  public NearSpansUnordered(final List<Spans> spans, final int slop)
  throws IOException {
    super(spans, slop);
    queue = new SpansQueue(spans.size());
  }

  @Override
  public boolean nextNode() throws IOException {
    firstTime = true;
    return super.nextNode();
  }

  @Override
  public boolean nextPosition() throws IOException {
    if (firstTime) {
      if (!this.initialiseSpansPosition()) {
        matchStart = matchEnd = PositionsIterator.NO_MORE_POS;
        return false;
      }
      firstTime = false;
    }
    else {
      if (!this.advanceMinToNextPosition()) {
        matchStart = matchEnd = PositionsIterator.NO_MORE_POS;
        return false;
      }
    }

    boolean isMatching;
    do {
      // while it is not matching, advance the min to the next position, and try again
      if (!(isMatching = isMatching())) {
        if (!this.advanceMinToNextPosition()) {
          matchStart = matchEnd = PositionsIterator.NO_MORE_POS;
          return false;
        }
      }
    } while (!isMatching);

    matchStart = queue.top().start();
    matchEnd = max.end();

    return isMatching;
  }

  /**
   * Initialise the spans by advancing them to their first position, compute the initial total length, and find the max.
   */
  private boolean initialiseSpansPosition() throws IOException {
    // reset totalLength to 0
    totalLength = 0;
    // reset max
    max = null;

    // Initialise the spans, compute the initial total length, and find the max
    for (Spans spans : subSpans) {
      if (!spans.nextPosition()) {
        return false;
      }
      // increment total length
      totalLength += spans.end() - spans.start();
      // update max reference
      this.updateMax(spans);
    }
    // rebuild the queue
    this.rebuildQueue();

    return true;
  }

  /**
   * Advance the min to its next position, update the total length and the max.
   */
  private boolean advanceMinToNextPosition() throws IOException {
    // retrieve the min
    Spans min = queue.top();
    // cache length before advancing to the next position
    int oldLength = min.end() - min.start();
    // we advance the min to its next position
    if (!min.nextPosition()) {
      return false;
    }
    // update total length
    this.updateTotalLength(oldLength, min.end() - min.start());
    // update max reference
    this.updateMax(min);
    // update queue
    queue.updateTop();

    return true;
  }

  private boolean isMatching() {
    return (this.getSlop() <= allowedSlop);
  }

  @Override
  protected int getSlop() {
    // retrieve the min
    Spans min = queue.top();
    return (max.end() - min.start()) - totalLength;
  }

  /**
   * Rebuild the queue
   */
  private void rebuildQueue() {
    queue.clear();
    for (Spans spans : subSpans) {
      queue.add(spans);
    }
  }

  /**
   * Check if the specified term spans is the max. If true, update the {@link #max} reference.
   * <p>
   * This method must be called whenever a spans is modified.
   */
  private void updateMax(final Spans spans) {
    if (max == null || spans.doc() > max.doc()) {
      max = spans;
      return;
    }

    // cache node comparison
    int nodeComparison = NodeUtils.compare(spans.node(), max.node());

    if ((spans.doc() == max.doc()) && (nodeComparison > 0)) {
      max = spans;
      return;
    }

    if ((spans.doc() == max.doc()) && (nodeComparison == 0) && (spans.end() > max.end())) {
      max = spans;
      return;
    }
  }

  private void updateTotalLength(final int oldLength, final int newLength) {
    if (oldLength != -1) {
      totalLength -= oldLength;  // subtract old length
    }
    totalLength += newLength; // add new length
  }

  private class SpansQueue extends PriorityQueue<Spans> {

    public SpansQueue(final int size) {
      super(size);
    }

    @Override
    protected final boolean lessThan(final Spans spans1, final Spans spans2) {
      return isSpansOrdered(spans1.start(), spans1.end(), spans2.start(), spans2.end());
    }

  }

}

