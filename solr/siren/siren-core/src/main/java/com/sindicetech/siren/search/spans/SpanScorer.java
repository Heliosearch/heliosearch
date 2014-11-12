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

import org.apache.lucene.search.Weight;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.search.node.NodeScorer;

import java.io.IOException;

/**
 * A {@link com.sindicetech.siren.search.node.NodeScorer} that matches spans of nodes.
 * <p>
 * This scorer relies on {@link com.sindicetech.siren.search.spans.Spans} for the computation of spans of nodes. This scorer
 * computes the frequency and score of a node spans. The score is computed as the sum of the spans' score.
 * <p>
 * Code taken from {@link org.apache.lucene.search.spans.SpanScorer} and adapted for the Siren use case.
 */
public class SpanScorer extends NodeScorer {

  protected Spans spans;
  protected int numMatches;
  protected float scoreInNode;
  protected boolean isScoreAndFreqComputed = false;

  protected SpanScorer(final Weight weight, final Spans spans) throws IOException {
    super(weight);
    this.spans = spans;
  }

  /**
   * The {@link Spans} that this scorer concerns.
   */
  protected Spans getSpans() {
    return this.spans;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    return spans.nextCandidateDocument();
  }

  @Override
  public boolean nextNode() throws IOException {
    // reset score and freq flag
    isScoreAndFreqComputed = false;

    do {
      // check if all the terms appear in the node
      if (!spans.nextNode()) {
        return false;
      }
      // we might have a match, call #nextPosition to check
    } while (!spans.nextPosition());

    return true;
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    return spans.skipToCandidate(target);
  }

  protected void computeScoreAndFreq() throws IOException {
    numMatches = 0;
    scoreInNode = 0.0f;

    // we have already called one time #nextPosition
    do {
      numMatches++;
      scoreInNode += spans.scoreInNode();
    } while (spans.nextPosition());

    // set the flag
    isScoreAndFreqComputed = true;
  }

  @Override
  public int doc() {
    return spans.doc();
  }

  @Override
  public IntsRef node() {
    return spans.node();
  }

  @Override
  public int freqInNode() throws IOException {
    if (!isScoreAndFreqComputed) {
      this.computeScoreAndFreq();
    }
    return numMatches;
  }

  @Override
  public float scoreInNode() throws IOException {
    if (!isScoreAndFreqComputed) {
      this.computeScoreAndFreq();
    }
    return scoreInNode;
  }

  @Override
  public long cost() {
    return spans.cost();
  }

}
