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
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeScorer;

import java.io.IOException;
import java.util.Set;

/**
 * Takes two {@link com.sindicetech.siren.search.spans.SpanQuery}, and returns all nodes that have instances of the
 * first Span that do not overlap with instances of the second or
 * within a x tokens before or y tokens after the second.
 * <p>
 * Code taken from {@link org.apache.lucene.search.spans.SpanNotQuery} and adapted for the Siren use case.
 */
public class NotSpanQuery extends SpanQuery {

  private SpanQuery include;

  private SpanQuery exclude;

  private final int pre;

  private final int post;

  protected class NotSpanWeight extends Weight {

    protected Similarity similarity;

    protected Weight includeWeight;
    protected Weight excludeWeight;

    public NotSpanWeight(final IndexSearcher searcher) throws IOException {
      // pass to child query the node constraints
      include.setNodeConstraint(lowerBound, upperBound);
      exclude.setNodeConstraint(lowerBound, upperBound);

      include.setLevelConstraint(levelConstraint);
      exclude.setLevelConstraint(levelConstraint);

      // transfer ancestor pointer to child
      include.setAncestorPointer(ancestor);
      exclude.setAncestorPointer(ancestor);

      this.includeWeight = include.createWeight(searcher);
      this.excludeWeight = exclude.createWeight(searcher);
    }

    @Override
    public String toString() {
      return "weight(" + NotSpanQuery.this + ")";
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
      final NodeScorer scorer = (NodeScorer) this.scorer(context, context.reader().getLiveDocs());
      if (scorer != null) {
        if (scorer.skipToCandidate(doc) && scorer.doc() == doc && scorer.nextNode()) {
          return includeWeight.explain(context, doc);
        }
      }
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }

    @Override
    public Query getQuery() {
      return NotSpanQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      // only use include clause
      float sum = this.includeWeight.getValueForNormalization();

      // boost each sub-weight
      sum *= NotSpanQuery.this.getBoost() * NotSpanQuery.this.getBoost();

      return sum;
    }

    @Override
    public void normalize(final float norm, float topLevelBoost) {
      // incorporate boost
      topLevelBoost *= NotSpanQuery.this.getBoost();
      // normalize all clauses
      this.includeWeight.normalize(norm, topLevelBoost);
      this.excludeWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      final Scorer includeScorer = this.includeWeight.scorer(context, acceptDocs);
      if (includeScorer == null) {
        return null;
      }
      if (!(includeScorer instanceof SpanScorer)) {
        throw new IllegalArgumentException("SpanScorer expected");
      }

      final Scorer excludeScorer = this.excludeWeight.scorer(context, acceptDocs);
      if (excludeScorer != null && !(excludeScorer instanceof SpanScorer)) {
        throw new IllegalArgumentException("SpanScorer expected");
      }

      // optimisation: if excluded is null, just use directly the includeScorer
      if (excludeScorer == null) {
        return includeScorer;
      }
      else {
        Spans includeSpans = ((SpanScorer) includeScorer).getSpans();
        Spans excludeSpans = ((SpanScorer) excludeScorer).getSpans();
        return new SpanScorer(this, new NotSpans(includeSpans, excludeSpans, pre, post));
      }
    }

  }

  /**
   * Construct a {@link NotSpanQuery} matching spans from include which have no overlap with spans from exclude.
   */
  public NotSpanQuery(SpanQuery include, SpanQuery exclude) {
    this.include = include;
    this.exclude = exclude;
    this.pre = 0;
    this.post = 0;
  }

  /**
   * Construct a {@link NotSpanQuery} matching spans from include which have no overlap with spans from exclude within
   * pre tokens before or post tokens of include.
   */
  public NotSpanQuery(SpanQuery include, SpanQuery exclude, int pre, int post) {
    this.include = include;
    this.exclude = exclude;
    this.pre = pre;
    this.post = post;
  }

  /**
   * Construct a {@link NotSpanQuery} matching spans from include which have no overlap with spans from exclude within
   * dist tokens of include.
   */
  public NotSpanQuery(SpanQuery include, SpanQuery exclude, int dist) {
    this.include = include;
    this.exclude = exclude;
    this.pre = dist;
    this.post = dist;
  }

  @Override
  public void setLevelConstraint(final int levelConstraint) {
    super.setLevelConstraint(levelConstraint);
    this.include.setLevelConstraint(levelConstraint);
    this.exclude.setLevelConstraint(levelConstraint);
  }

  @Override
  public void setNodeConstraint(final int lowerBound, final int upperBound) {
    super.setNodeConstraint(lowerBound, upperBound);
    // keep clauses synchronised
    this.include.setNodeConstraint(lowerBound, upperBound);
    this.exclude.setNodeConstraint(lowerBound, upperBound);
  }

  @Override
  public void setAncestorPointer(final NodeQuery ancestor) {
    super.setAncestorPointer(ancestor);
    // keep clauses synchronised
    this.include.setAncestorPointer(ancestor);
    this.exclude.setAncestorPointer(ancestor);
  }

  public SpanQuery getInclude() {
    return this.include;
  }

  public SpanQuery getExclude() {
    return this.exclude;
  }

  public int getPre() {
    return this.pre;
  }

  public int getPost() {
    return this.post;
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    return new NotSpanWeight(searcher);
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    include.extractTerms(terms);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanNot(");
    buffer.append(include.toString(field));
    buffer.append(", ");
    buffer.append(exclude.toString(field));
    buffer.append(", ");
    buffer.append(Integer.toString(pre));
    buffer.append(", ");
    buffer.append(Integer.toString(post));
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    NotSpanQuery clone = null;

    SpanQuery rewrittenInclude = (SpanQuery) include.rewrite(reader);
    if (rewrittenInclude != include) {
      clone = this.clone();
      clone.include = rewrittenInclude;
    }
    SpanQuery rewrittenExclude = (SpanQuery) exclude.rewrite(reader);
    if (rewrittenExclude != exclude) {
      if (clone == null) {
        clone = this.clone();
      }
      clone.exclude = rewrittenExclude;
    }

    if (clone != null) {
      // transfer constraints
      clone.setNodeConstraint(lowerBound, upperBound);
      clone.setLevelConstraint(levelConstraint);
      // transfer ancestor pointer
      clone.setAncestorPointer(ancestor);
      return clone; // some clauses rewrote
    }
    else {
      return this; // no clauses rewrote
    }
  }

  @Override
  public NotSpanQuery clone() {
    final NotSpanQuery clone = (NotSpanQuery) super.clone();
    clone.include = (SpanQuery) this.include.clone();
    clone.exclude = (SpanQuery) this.exclude.clone();
    return clone;
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(this.getBoost())
            ^ ((exclude == null) ? 0 : exclude.hashCode())
            ^ ((include == null) ? 0 : include.hashCode())
            ^ post
            ^ pre
            ^ levelConstraint
            ^ upperBound
            ^ lowerBound;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NotSpanQuery)) return false;
    final NotSpanQuery other = (NotSpanQuery) o;
    return (this.getBoost() == other.getBoost()) &&
            this.include != null && this.include.equals(other.include) &&
            this.exclude != null && this.exclude.equals(other.exclude) &&
            this.pre == other.pre &&
            this.post == other.post &&
            this.levelConstraint == other.levelConstraint &&
            this.lowerBound == other.lowerBound &&
            this.upperBound == other.upperBound;
  }

}
