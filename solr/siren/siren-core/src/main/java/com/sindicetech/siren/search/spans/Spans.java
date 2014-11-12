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

import java.io.IOException;

/**
 * Expert: an enumeration of span matches. Used to implement span searching.
 * Each span represents a range of term or node positions within a document. Matches
 * are enumerated in order, by increasing document identifier, within that by
 * increasing node identifier, within that by start position and finally by
 * increasing end position.
 *
 * <p>
 *
 * Each spans is responsible of computing its score. For example, {@link com.sindicetech.siren.search.spans.NearSpans}
 * applies weight based on the slop of the current match to its score.
 *
 * <p>
 *
 * Code taken from {@link org.apache.lucene.search.spans.Spans} and adapted for the Siren use case.
 */
abstract class Spans {

  /**
   * Move to the next candidate document, returning true if any such exists.
   */
  public abstract boolean nextCandidateDocument() throws IOException;

  /**
   * Move to the next node in the current document, returning true if any such exists.
   * <p>
   * Should not be called until {@link #nextCandidateDocument()} or
   * {@link #skipToCandidate(int)} are called for the first time.
   *
   * @return false if there is no more node for the current entity or if
   * {@link #nextCandidateDocument()} or {@link #skipToCandidate(int)} were not
   * called yet.
   */
  public abstract boolean nextNode() throws IOException;

  /**
   * Move to the next position in the current node.
   * <p>
   * Should not be called until {@link #nextNode()} is called for the first
   * time.
   *
   * @return false if there is no more position for the current node or if
   * {@link #nextNode()} was not called yet.
   */
  public abstract boolean nextPosition() throws IOException;

  /**
   * Skip to the first candidate document beyond (see NOTE below) the current
   * whose number is greater than or equal to <i>target</i>. Returns false if
   * there are no more docs in the set.
   * <p>
   * <b>NOTE:</b> when <code> target &le; current</code> implementations must
   * not advance beyond their current {@link #doc()}.
   */
  public abstract boolean skipToCandidate(final int target) throws IOException;

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link org.apache.lucene.search.Scorer#NO_MORE_DOCS} if {@link #nextCandidateDocument()} or
   * {@link #skipToCandidate(int)} were not called yet.
   * <li>{@link org.apache.lucene.search.Scorer##NO_MORE_DOCS} if the iterator has exhausted.
   * <li>Otherwise it should return the document identifier of the current match.
   * </ul>
   * <p>
   */
  public abstract int doc();

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link com.sindicetech.siren.index.DocsAndNodesIterator##NO_MORE_NOD} if {@link #nextNode()} was not called
   * yet.
   * <li>{@link com.sindicetech.siren.index.DocsAndNodesIterator##NO_MORE_NOD} if the iterator has exhausted.
   * <li>Otherwise it should return the node of the current match.
   * </ul>
   */
  public abstract IntsRef node();

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link com.sindicetech.siren.index.PositionsIterator#NO_MORE_POS} if {@link #nextPosition()} was not called
   * yet.
   * <li>{@link com.sindicetech.siren.index.PositionsIterator#NO_MORE_POS} if the iterator has exhausted.
   * <li>Otherwise it should return the the start position of the current match.
   * </ul>
   */
  public abstract int start();

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link com.sindicetech.siren.index.PositionsIterator#NO_MORE_POS} if {@link #nextPosition()} was not called
   * yet.
   * <li>{@link com.sindicetech.siren.index.PositionsIterator#NO_MORE_POS} if the iterator has exhausted.
   * <li>Otherwise it should return the the end position of the current match.
   * </ul>
   */
  public abstract int end();

  /**
   * Returns the score of the node in the current span match.
   * <p>
   * Should not be called until {@link #nextPosition()} is called for the first time.
   */
  public abstract float scoreInNode() throws IOException;

  /**
   * Returns the estimated cost of this spans.
   * <p>
   * This is generally an upper bound of the number of documents this iterator
   * might match, but may be a rough heuristic, hardcoded value, or otherwise
   * completely inaccurate.
   */
  // TODO: new cost API introduced in LUCENE-4607
  public long cost() {
    throw new UnsupportedOperationException();
  }

  /**
   * Computes a weight based on a slop. The weight will decrease as the slop becomes large.
   * Implemented as <code>1 / (slop + 1)</code>.
   */
  protected float sloppyWeight(int slop) {
    return 1.0f / (slop + 1);
  }

  /**
   * Returns the slop of the current match.
   */
  protected abstract int getSlop();

}
