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

package com.sindicetech.siren.search.node;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;

/**
 * The abstract {@link Scorer} class that defines the interface for iterating
 * over an ordered list of nodes matching a query.
 * <p>
 * Subclasses should implement {@link #docID()}, {@link #nextDoc()} and
 * {@link #advance(int)} for compatibility with {@link Scorer} if needed.
 */
public abstract class NodeScorer extends Scorer {

  protected NodeScorer(final Weight weight) {
    super(weight);
  }

  /**
   * Advances to the next candidate document in the set, or returns false if
   * there are no more docs in the set.
   * <p>
   * A candidate document is a document that represents a potential match for
   * the query. All the sub-scorers agree on its document id. However finding
   * the matching node within this document still needs to be done. If the
   * sub-scorers do not agree on a node by calling {@link #nextNode()}, the
   * document must be considered as a non-matching document.
   * <p>
   * This method is useful for optimisation, i.e., lazy loading and comparison
   * of node information. It allows to iterate over documents and find candidate
   * without loading node information. Node information are loaded and compared
   * only when a candidate is found.
   */
  public abstract boolean nextCandidateDocument() throws IOException;

  /**
   * Move to the next node path in the current document matching the query.
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
   * Skip to the first candidate document beyond (see NOTE below) the current
   * whose number is greater than or equal to <i>target</i>. Returns false if
   * there are no more docs in the set.
   * <p>
   * <b>NOTE:</b> when <code> target &le; current</code> implementations must
   * not advance beyond their current {@link #doc()}.
   */
  public abstract boolean skipToCandidate(int target) throws IOException;

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link #NO_MORE_DOCS} if {@link #nextCandidateDocument()} or
   * {@link #skipToCandidate(int)} were not called yet.
   * <li>{@link #NO_MORE_DOCS} if the iterator has exhausted.
   * <li>Otherwise it should return the doc ID it is currently on.
   * </ul>
   * <p>
   */
  public abstract int doc();

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link com.sindicetech.siren.index.DocsAndNodesIterator#NO_MORE_NOD} if {@link #nextNode()} was not called
   * yet.
   * <li>{@link com.sindicetech.siren.index.DocsAndNodesIterator#NO_MORE_NOD} if the iterator has exhausted.
   * <li>Otherwise it should return the node it is currently on.
   * </ul>
   */
  public abstract IntsRef node();

  /**
   * Returns the number of occurrences in the current node
   */
  public abstract int freqInNode() throws IOException;

  /**
   * Returns the score of the current node of the current
   * document matching the query.
   */
  public abstract float scoreInNode() throws IOException;

  /**
   * Methods implemented in {@link LuceneProxyNodeScorer}
   */

  @Override
  public float score() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int freq() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int docID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextDoc() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int advance(final int target) throws IOException {
    throw new UnsupportedOperationException();
  }

  // TODO: new cost API introduced in LUCENE-4607
  @Override
  public long cost() {
    throw new UnsupportedOperationException();
  }

}
