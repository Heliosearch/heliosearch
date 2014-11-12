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

import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;

/**
 * Abstraction of a {@link NodeScorer} for {@link NodePhraseQuery}.
 *
 * <p>
 *
 * A node is considered matching if it contains the phrase-query terms at
 * "valid" positions. What "valid positions" are depends on the type of the
 * phrase query: for an exact phrase query terms are required to appear in
 * adjacent locations. The method {@link #phraseFreq()}, when invoked, compute all
 * the occurrences of the phrase within the node. A non
 * zero frequency means a match.
 */
abstract class NodePhraseScorer extends NodeScorer {

  NodePhrasePosition[] phrasePositions;

  final Similarity.SimScorer simScorer;

  final NodeConjunctionScorer conjunctionScorer;

  /**
   * Phrase frequency in current doc as computed by phraseFreq().
   */
  private int freq = 0;

  NodePhraseScorer(final Weight weight,
                   final NodePhraseQuery.PostingsAndPosition[] postings,
                   final Similarity.SimScorer simScorer)
  throws IOException {
    super(weight);
    this.simScorer = simScorer;

    // convert tps to a list of phrase positions.
    // note: phrase-position differs from term-position in that its position
    // reflects the phrase offset: pp.pos = tp.pos - offset.
    // this allows to easily identify a matching (exact) phrase
    // when all PhrasePositions have exactly the same position.
    phrasePositions = new NodePhrasePosition[postings.length];
    for (int i = 0; i < postings.length; i++) {
      phrasePositions[i] = new NodePhrasePosition(postings[i].postings, postings[i].position);
    }

    // create node conjunction scorer
    final NodeScorer[] scorers = new NodeScorer[postings.length];
    for (int i = 0; i < postings.length; i++) {
      scorers[i] = new NodeTermScorer(weight, postings[i].postings, this.simScorer);
    }
    conjunctionScorer = new NodeConjunctionScorer(weight, 1.0f, scorers);
  }

  @Override
  public int doc() {
    return conjunctionScorer.doc();
  }

  @Override
  public IntsRef node() {
    return conjunctionScorer.node();
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    freq = 0; // reset freq
    return conjunctionScorer.nextCandidateDocument();
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    return conjunctionScorer.skipToCandidate(target);
  }

  @Override
  public boolean nextNode() throws IOException {
    freq = 0; // reset freq
    while (conjunctionScorer.nextNode()) { // if node contains phrase-query terms
      if (this.firstPhrase()) { // check for phrase
        // TODO: Instead of computing the full phraseFreq, we could defer it until freqInNode or scoreInNode is called.
        freq = this.phraseFreq(); // compute frequency of the phrase
        return true;
      }
    }
    return false;
  }

  abstract boolean firstPhrase() throws IOException;

  abstract boolean nextPhrase() throws IOException;

  abstract int phraseFreq() throws IOException;

  /**
   * phrase frequency in current node as computed by phraseFreq().
   */
  @Override
  public int freqInNode()
  throws IOException {
    return freq;
  }

  @Override
  public float scoreInNode()
  throws IOException {
    return conjunctionScorer.scoreInNode();
  }

}
