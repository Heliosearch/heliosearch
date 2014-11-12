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

import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeScorer;

import java.io.IOException;
import java.util.Set;

/**
 * Base class for node-based span queries that is a wrapper around {@link com.sindicetech.siren.search.node.NodeQuery}.
 * <p>
 * A node-based span query matches spans of nodes. The span is relative to the nodes returned by the
 * {@link com.sindicetech.siren.search.node.NodeQuery}. The node returned by this span is the parent node of the node
 * returned by the {@link com.sindicetech.siren.search.node.NodeQuery}, and the start and end position of the span is based
 * on the index of the child node.
 * <p>
 * Code taken from {@link org.apache.lucene.search.spans.SpanTermQuery} and adapted for the Siren use case.
 *
 * @see com.sindicetech.siren.search.spans.NodeSpans
 */
public class NodeSpanQuery extends SpanQuery {

  /**
   * The node query that defines the node spans
   */
  protected NodeQuery query;

  protected class NodeSpanWeight extends Weight {

    private Weight weight;

    public NodeSpanWeight(final IndexSearcher searcher) throws IOException {
      this.weight = NodeSpanQuery.this.query.createWeight(searcher);
    }

    @Override
    public String toString() {
      return "weight(" + NodeSpanQuery.this + ")";
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
      return NodeSpanQuery.this;
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
      if (!(scorer instanceof NodeScorer)) {
        throw new IllegalArgumentException("NodeScorer expected");
      }
      NodeSpans spans = new NodeSpans((NodeScorer) scorer);
      return new SpanScorer(this, spans);
    }

  }

  /** Construct a NodeSpanQuery matching the named node's spans. */
  public NodeSpanQuery(NodeQuery query) { this.query = query; }

  @Override
  public void setLevelConstraint(final int levelConstraint) {
    super.setLevelConstraint(levelConstraint);
    this.query.setLevelConstraint(levelConstraint);
  }

  @Override
  public void setNodeConstraint(final int lowerBound, final int upperBound) {
    super.setNodeConstraint(lowerBound, upperBound);
    this.query.setNodeConstraint(lowerBound, upperBound);
  }

  @Override
  public void setAncestorPointer(final NodeQuery ancestor) {
    super.setAncestorPointer(ancestor);
    this.query.setAncestorPointer(ancestor);
  }

  /** Return the query whose spans are matched. */
  public NodeQuery getQuery() { return query; }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    return new NodeSpanWeight(searcher);
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    query.extractTerms(terms);
  }

  @Override
  public String toString(final String field) {
    return query.toString(field);
  }

  @Override
  public Query rewrite(final IndexReader reader) throws IOException {
    NodeSpanQuery clone = null;

    NodeQuery rewritten = (NodeQuery) this.query.rewrite(reader);
    if (rewritten != this.query) {
      clone = this.clone();

      // transfer constraints
      rewritten.setNodeConstraint(lowerBound, upperBound);
      rewritten.setLevelConstraint(levelConstraint);
      // transfer ancestor pointer
      rewritten.setAncestorPointer(ancestor);

      clone.query = rewritten;
    }

    if (clone != null) {
      return clone; // some clauses rewrote
    }
    else {
      return this; // no clauses rewrote
    }
  }

  @Override
  public NodeSpanQuery clone() {
    final NodeSpanQuery clone = (NodeSpanQuery) super.clone();
    clone.query = (NodeQuery) this.query.clone();
    return clone;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Float.floatToIntBits(this.getBoost());
    result = prime * result + query.hashCode();
    result = prime * result + lowerBound;
    result = prime * result + upperBound;
    result = prime * result + levelConstraint;
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof NodeSpanQuery)) return false;
    final NodeSpanQuery other = (NodeSpanQuery) o;
    return (this.getBoost() == other.getBoost()) &&
            this.query.equals(other.query) &&
            this.levelConstraint == other.levelConstraint &&
            this.lowerBound == other.lowerBound &&
            this.upperBound == other.upperBound;
  }

}
