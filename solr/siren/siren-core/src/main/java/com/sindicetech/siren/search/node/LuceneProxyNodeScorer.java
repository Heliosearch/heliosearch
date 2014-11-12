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

import java.io.IOException;
import java.util.Collection;

/**
 * The {@link Scorer} class that defines the interface for iterating
 * over an ordered list of documents matching a {@link NodeQuery}.
 */
class LuceneProxyNodeScorer extends Scorer {

  private int              lastDoc = -1;
  private float            score;
  private int              freq;
  private final NodeScorer scorer;

  public LuceneProxyNodeScorer(final NodeScorer scorer) {
    super(scorer.getWeight());
    this.scorer = scorer;
  }

  @Override
  public int docID() {
    return scorer.doc();
  }

  @Override
  public int advance(final int target) throws IOException {
    if (scorer.skipToCandidate(target)) {
      do {
        if (scorer.nextNode()) {
          return this.docID();
        }
      } while (scorer.nextCandidateDocument());
    }
    return NO_MORE_DOCS;
  }

  @Override
  public int nextDoc() throws IOException {
    while (scorer.nextCandidateDocument()) {
      if (scorer.nextNode()) { // check if there is at least 1 node that matches the query
        return this.docID();
      }
    }
    return NO_MORE_DOCS;
  }

  @Override
  public float score() throws IOException {
    this.computeScoreAndFreq();
    return score;
  }

  /**
   * Returns number of matches for the current document.
   * <p>
   * Only valid after calling {@link #nextDoc()} or {@link #advance(int)}
   */
  @Override
  public int freq() throws IOException {
    this.computeScoreAndFreq();
    return freq;
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return scorer.getChildren();
  }

  /**
   * Compute the score and the frequency of the current document
   * @throws IOException
   */
  private void computeScoreAndFreq() throws IOException {
    final int doc = this.docID();

    if (doc != lastDoc) {
      lastDoc = doc;
      score = 0;
      freq = 0;

      do { // nextNode() was already called in nextDoc() or in advance()
        score += scorer.scoreInNode();
        freq += scorer.freqInNode();
      } while (scorer.nextNode());
    }
  }

  @Override
  public long cost() {
    // TODO: hardcoded value for the moment, until #265 is solved
    return 1;
  }

}
