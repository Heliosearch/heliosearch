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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeScorer;

import java.io.IOException;
import java.util.Set;

/**
 * Base class for filtering a {@link com.sindicetech.siren.search.spans.SpanQuery} based on the position of a match.
 */
public abstract class PositionCheckSpanQuery extends SpanQuery {

  protected SpanQuery match;

  protected class PositionCheckSpanWeight extends Weight {

    private Weight weight;

    public PositionCheckSpanWeight(final IndexSearcher searcher) throws IOException {
      this.weight = PositionCheckSpanQuery.this.match.createWeight(searcher);
    }

    @Override
    public String toString() {
      return "weight(" + PositionCheckSpanQuery.this + ")";
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
      final NodeScorer scorer = (NodeScorer) this.scorer(context, context.reader().getLiveDocs());
      if (scorer != null) {
        if (scorer.skipToCandidate(doc) && scorer.doc() == doc) {
          return weight.explain(context, doc);
        }
      }
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }

    @Override
    public Query getQuery() {
      return PositionCheckSpanQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return this.weight.getValueForNormalization();
    }

    @Override
    public void normalize(final float norm, final float topLevelBoost) {
      this.weight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      Scorer scorer = this.weight.scorer(context, acceptDocs);
      if (scorer == null) {
        return null;
      }
      if (!(scorer instanceof SpanScorer)) {
        throw new IllegalArgumentException("SpanScorer expected");
      }
      PositionCheckSpans spans = new PositionCheckSpans((SpanScorer) scorer);
      return new SpanScorer(this, spans);
    }

  }

  public PositionCheckSpanQuery(SpanQuery match) {
    this.match = match;
  }

  @Override
  public void setLevelConstraint(final int levelConstraint) {
    super.setLevelConstraint(levelConstraint);
    this.match.setLevelConstraint(levelConstraint);
  }

  @Override
  public void setNodeConstraint(final int lowerBound, final int upperBound) {
    super.setNodeConstraint(lowerBound, upperBound);
    this.match.setNodeConstraint(lowerBound, upperBound);
  }

  @Override
  public void setAncestorPointer(final NodeQuery ancestor) {
    super.setAncestorPointer(ancestor);
    this.match.setAncestorPointer(ancestor);
  }

  /**
   * @return the {@link com.sindicetech.siren.search.spans.SpanQuery} whose matches are filtered.
   */
  public SpanQuery getMatch() { return match; }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    return new PositionCheckSpanWeight(searcher);
  }

  @Override
  public void extractTerms(Set<Term> terms) { match.extractTerms(terms); }

  /**
   * Return value for {@link PositionCheckSpanQuery#acceptPosition(Spans)}.
   */
  protected static enum AcceptStatus {
    //Indicates the match should be accepted
    YES,
    //Indicates the match should be rejected, and we advance to next position
    NO
  };

  /**
   * Implementing classes are required to return whether the current position is a match for the passed in "match"
   * {@link com.sindicetech.siren.search.spans.SpanQuery}.
   * This is only called if the underlying {@link Spans#nextPosition()} for the match is successful
   *
   * @param spans The {@link Spans} instance, positioned at the spot to check
   * @return whether the match is accepted, or rejected and should move to the next position.
   */
  protected abstract AcceptStatus acceptPosition(Spans spans) throws IOException;

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    PositionCheckSpanQuery clone = null;
    SpanQuery rewritten = (SpanQuery) match.rewrite(reader);
    if (rewritten != match) {
      clone = (PositionCheckSpanQuery) this.clone();

      // transfer constraints
      rewritten.setNodeConstraint(lowerBound, upperBound);
      rewritten.setLevelConstraint(levelConstraint);
      // transfer ancestor pointer
      rewritten.setAncestorPointer(ancestor);

      clone.match = rewritten;
    }

    if (clone != null) {
        return clone;                        // some clauses rewrote
    } else {
        return this;                         // no clauses rewrote
    }
  }

  protected class PositionCheckSpans extends Spans {

    private Spans spans;

    public PositionCheckSpans(SpanScorer scorer) {
      this.spans = scorer.getSpans();
    }

    @Override
    public boolean nextCandidateDocument() throws IOException {
      return spans.nextCandidateDocument();
    }

    @Override
    public boolean skipToCandidate(int target) throws IOException {
      return spans.skipToCandidate(target);
    }

    @Override
    public boolean nextNode() throws IOException {
      return spans.nextNode();
    }

    @Override
    public boolean nextPosition() throws IOException {
      if (!spans.nextPosition()){
        return false;
      }
      return doNextPosition();
    }

    protected boolean doNextPosition() throws IOException {
      for (;;) {
        switch (acceptPosition(this)) {
          case YES:
            return true;
          case NO:
            if (!spans.nextPosition())
              return false;
            break;
         }
      }
    }

    @Override
    public float scoreInNode() throws IOException {
      return spans.scoreInNode();
    }

    @Override
    public int getSlop() {
      return spans.getSlop();
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
    public int start() {
      return spans.start();
    }

    @Override
    public int end() {
      return spans.end();
    }

    @Override
    public String toString() {
      return "spans(" + PositionCheckSpanQuery.this.toString() + ")";
    }

    @Override
    public long cost() {
      return spans.cost();
    }

  }
}
