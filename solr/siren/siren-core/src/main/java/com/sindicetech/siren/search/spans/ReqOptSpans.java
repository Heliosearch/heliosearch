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

import com.sindicetech.siren.util.NodeUtils;

import java.io.IOException;
import java.util.List;

/**
 * A {@link Spans}, created by the {@link com.sindicetech.siren.search.spans.BooleanSpans}, for matching a required
 * and an optional spans clause. It defers the enumeration of the optional spans until a score is needed. If it finds
 * a matching optional span, it integrates the score of the optional spans with the score of the required spans.
 * <p>
 * The optional spans are subject to the slop defined by the
 * {@link com.sindicetech.siren.search.spans.BooleanSpanQuery}. If the optional span does not satisfy the slop constraint,
 * it is not counted as a match.
 * <p>
 * The optional spans is allowed to overlap with the required spans.
 * <p>
 * This {@link Spans} is created by the {@link com.sindicetech.siren.search.spans.BooleanSpans}
 *
 * @see com.sindicetech.siren.search.spans.BooleanSpans
 */
class ReqOptSpans extends Spans {

  private final Spans requiredSpans;
  private final Spans optionalSpans;
  private final int allowedSlop;
  private final List<Spans> originalRequiredSpans;

  private boolean isOptionalExhausted = false;

  /**
   * Construct a spans with a required and an optional clauses.
   * <p>
   * We need a reference to the {@link com.sindicetech.siren.search.spans.BooleanSpans} parent, as we need to access the
   * original list of required clauses in order to properly compute the score.
   */
  ReqOptSpans(final Spans includeSpans, final Spans optionalSpans, final BooleanSpans parent)
  throws IOException {
    this.requiredSpans = includeSpans;
    this.optionalSpans = optionalSpans;
    this.allowedSlop = parent.slop;
    this.originalRequiredSpans = parent.requiredSpans;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    return this.requiredSpans.nextCandidateDocument();
  }

  @Override
  public boolean nextNode() throws IOException {
    return this.requiredSpans.nextNode();
  }

  @Override
  public boolean nextPosition() throws IOException {
    return this.requiredSpans.nextPosition();
  }

  @Override
  public boolean skipToCandidate(int target) throws IOException {
    return this.requiredSpans.skipToCandidate(target);
  }

  @Override
  public float scoreInNode() throws IOException {
    final float reqScore = requiredSpans.scoreInNode();
    final int doc = requiredSpans.doc();

    if (isOptionalExhausted) {
      return reqScore;
    }
    // if it is the first call, optScorer.doc() should return -1
    else if (optionalSpans.doc() < doc && !optionalSpans.skipToCandidate(doc)) {
      isOptionalExhausted = true;
      return reqScore;
    }

    final IntsRef reqNode = this.node();
    /*
     * the optional scorer can be in a node that is before the one where
     * the required scorer is in.
     */
    int cmp = -1;
    while ((cmp = NodeUtils.compare(optionalSpans.node(), reqNode)) < 0) {
      // if nodes are exhausted in the optional spans, returns the reqScore
      if (!optionalSpans.nextNode()) {
        return reqScore;
      }
    }

    // If the node in the optional spans is after the required, returns the reqScore
    if (optionalSpans.doc() == doc && cmp > 0) {
      return reqScore;
    }

    // Both the required and optional are positioned on the same node, we needs to validate the position of the
    // optional spans

    int allowedOptionalSlop = allowedSlop - requiredSpans.getSlop();

    // while optional positions are not exhausted, and while the optional start is not past the allowed optional slop
    // after the end of the required spans
    while (optionalSpans.nextPosition() && optionalSpans.start() <= (requiredSpans.end() + allowedOptionalSlop)) {
      // if the slop of the current optional match is too large, advance to the next optional position
      int optionalSlop = this.getOptionalSlop();
      // the optional slop is smaller than the allowed optional slop, we have a match
      if (optionalSlop <= allowedOptionalSlop) {
        return this.getReqOptScore();
      }
    }

    return reqScore;
  }

  /**
   * Returns the slop between the required and optional clauses.
   */
  private int getOptionalSlop() {
    int totalLength = requiredSpans.end() - requiredSpans.start();
    totalLength += optionalSpans.end() - optionalSpans.start();
    Spans min = optionalSpans.start() < requiredSpans.start() ? optionalSpans : requiredSpans;
    Spans max = optionalSpans.end() < requiredSpans.end() ? requiredSpans : optionalSpans;
    return (max.end() - min.start()) - totalLength;
  }

  /**
   * If an optional clause matches, compute the score of the required and optional clause. We need to recompute the
   * score of the required clause as the sloppy weight might have changed due to the optional match.
   */
  private float getReqOptScore() throws IOException {
    int totalLength = 0;
    float score = 0.0f;

    for (Spans spans : originalRequiredSpans) {
      totalLength += spans.end() - spans.start();
      score += spans.scoreInNode();
    }
    totalLength += optionalSpans.end() - optionalSpans.start();
    score += optionalSpans.scoreInNode();

    Spans min = optionalSpans.start() < requiredSpans.start() ? optionalSpans : requiredSpans;
    Spans max = optionalSpans.end() < requiredSpans.end() ? requiredSpans : optionalSpans;
    int slop = (max.end() - min.start()) - totalLength;
    return score * this.sloppyWeight(slop);
  }

  @Override
  public int getSlop() {
    return requiredSpans.getSlop();
  }

  @Override
  public int doc() {
    return requiredSpans.doc();
  }

  @Override
  public IntsRef node() {
    return requiredSpans.node();
  }

  @Override
  public int start() {
    return requiredSpans.start();
  }

  @Override
  public int end() {
    return requiredSpans.end();
  }

}
