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
import org.apache.lucene.util.ToStringUtils;

import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodeQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A {@link com.sindicetech.siren.search.spans.SpanQuery} that matches a boolean combination of spans queries, e.g.,
 * {@link com.sindicetech.siren.search.spans.TermSpanQuery}s, {@link com.sindicetech.siren.search.spans.NodeSpanQuery}s,
 * {@link com.sindicetech.siren.search.spans.BooleanSpanQuery}s,
 * ...
 *
 * <p>
 *
 * The logic of this class overlaps with {@link com.sindicetech.siren.search.node.NodeBooleanQuery}. Since multi-inheritance
 * is not allowed, and therefore we cannot both extends {@link com.sindicetech.siren.search.spans.SpanQuery} and
 * {@link com.sindicetech.siren.search.node.NodeBooleanQuery}, we use composition as a workaround. This class extends
 * {@link com.sindicetech.siren.search.spans.SpanQuery} and relates to
 * {@link com.sindicetech.siren.search.node.NodeBooleanQuery} by composition to which it delegates most
 * of the method calls.
 */
public class BooleanSpanQuery extends SpanQuery {

  /**
   * A reference to the {@link com.sindicetech.siren.search.node.NodeBooleanQuery} instance
   */
  private NodeBooleanQuery nodeBooleanQuery;

  private int slop;

  private boolean inOrder;

  /** Constructs an empty boolean span query. */
  public BooleanSpanQuery(final int slop, final boolean inOrder) {
    this.nodeBooleanQuery = new NodeBooleanQuery();
    this.slop = slop;
    this.inOrder = inOrder;
  }

  @Override
  public void setLevelConstraint(final int levelConstraint) {
    super.setLevelConstraint(levelConstraint);
    this.nodeBooleanQuery.setLevelConstraint(levelConstraint);
  }

  @Override
  public void setNodeConstraint(final int lowerBound, final int upperBound) {
    super.setNodeConstraint(lowerBound, upperBound);
    this.nodeBooleanQuery.setNodeConstraint(lowerBound, upperBound);
  }

  @Override
  public void setAncestorPointer(final NodeQuery ancestor) {
    super.setAncestorPointer(ancestor);
    this.nodeBooleanQuery.setAncestorPointer(ancestor);
  }

  /**
   * Adds a clause to a boolean span query.
   *
   * @throws com.sindicetech.siren.search.node.NodeBooleanQuery.TooManyClauses
   *           if the new number of clauses exceeds the maximum clause number
   * @see com.sindicetech.siren.search.node.NodeBooleanQuery#getMaxClauseCount()
   */
  public void add(final SpanQuery query, final NodeBooleanClause.Occur occur) {
    this.nodeBooleanQuery.add(query, occur);
  }

  /** Return the maximum number of intervening unmatched positions permitted.*/
  public int getSlop() { return slop; }

  /** Return true if matches are required to be in-order.*/
  public boolean isInOrder() { return inOrder; }

  /** Returns the set of clauses in this query. */
  public NodeBooleanClause[] getClauses() {
    return this.nodeBooleanQuery.getClauses();
  }

  /** Returns the list of clauses in this query. */
  public List<NodeBooleanClause> clauses() {
    return this.nodeBooleanQuery.clauses();
  }

  /**
   * Returns an iterator on the clauses in this query. It implements the
   * {@link Iterable} interface to make it possible to do:
   * <pre>for (SirenBooleanClause clause : booleanQuery) {}</pre>
   */
  public final Iterator<NodeBooleanClause> iterator() {
    return this.nodeBooleanQuery.iterator();
  }

  /**
   * Expert: the Weight for {@link com.sindicetech.siren.search.spans.BooleanSpanQuery}, used to
   * normalize, score and explain these queries.
   */
  public class BooleanSpanWeight extends Weight {

    private NodeBooleanQuery.NodeBooleanWeight nodeBooleanWeight;

    public BooleanSpanWeight(final IndexSearcher searcher) throws IOException {
      this.nodeBooleanWeight = (NodeBooleanQuery.NodeBooleanWeight) BooleanSpanQuery.this.nodeBooleanQuery.createWeight(searcher);
    }

    @Override
    public String toString() {
      return "weight(" + BooleanSpanQuery.this + ")";
    }

    @Override
    public Query getQuery() {
      return BooleanSpanQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return this.nodeBooleanWeight.getValueForNormalization();
    }

    @Override
    public void normalize(final float norm, float topLevelBoost) {
      this.nodeBooleanWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc)
    throws IOException {
      return this.nodeBooleanWeight.explain(context, doc);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs)
    throws IOException {
      final List<Spans> required = new ArrayList<Spans>();
      final List<Spans> prohibited = new ArrayList<Spans>();
      final List<Spans> optional = new ArrayList<Spans>();
      final Iterator<NodeBooleanClause> cIter = nodeBooleanQuery.iterator();
      final Iterator<Weight> wIter = this.nodeBooleanWeight.getWeights().iterator();
      while (wIter.hasNext()) {
        NodeBooleanClause c = cIter.next();
        final SpanScorer subScorer = (SpanScorer) wIter.next().scorer(context, acceptDocs);
        if (subScorer == null) {
          if (c.isRequired()) {
            return null;
          }
        }
        else if (c.isRequired()) {
          required.add(subScorer.getSpans());
        }
        else if (c.isProhibited()) {
          prohibited.add(subScorer.getSpans());
        }
        else {
          optional.add(subScorer.getSpans());
        }
      }

      if (required.size() == 0 && optional.size() == 0) {
        // no required and optional clauses.
        return null;
      }

      return new SpanScorer(this, new BooleanSpans(required, optional, prohibited, slop, inOrder));
    }

  }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    return new BooleanSpanWeight(searcher);
  }

  @Override
  public Query rewrite(final IndexReader reader) throws IOException {
    BooleanSpanQuery clone = null;

    final NodeQuery query = (NodeQuery) this.nodeBooleanQuery.rewrite(reader);
    if (query != this.nodeBooleanQuery) { // clause rewrote: must clone
      clone = (BooleanSpanQuery) this.clone();
      if (query instanceof NodeBooleanQuery) {
        clone.nodeBooleanQuery = (NodeBooleanQuery) query;
        return clone;
      }
      else { // not a boolean query anymore, must have been a 1-clause query
        return query;
      }
    }

    // no clauses rewrote
    return this;
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    this.nodeBooleanQuery.extractTerms(terms);
  }

  @Override @SuppressWarnings("unchecked")
  public Query clone() {
    final BooleanSpanQuery clone = (BooleanSpanQuery) super.clone();
    clone.nodeBooleanQuery = (NodeBooleanQuery) this.nodeBooleanQuery.clone();
    return clone;
  }

  @Override
  public String toString(final String field) {
    String str = this.nodeBooleanQuery.toString(field);

    final boolean hasBoost = (this.getBoost() != 1.0);
    if (hasBoost) { // remove boost
      int i = str.lastIndexOf('^');
      str = str.substring(0, i);
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(str);

    if (inOrder) { // append slop
      builder.append('#');
    }
    else {
      builder.append('~');
    }
    builder.append(slop);
    if (hasBoost) { // append boost
      builder.append(ToStringUtils.boost(this.getBoost()));
    }
    return builder.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof BooleanSpanQuery)) return false;
    final BooleanSpanQuery other = (BooleanSpanQuery) o;
    return (this.nodeBooleanQuery.equals(other.nodeBooleanQuery) &&
            this.slop == other.slop &&
            this.inOrder == other.inOrder);
  }

  @Override
  public int hashCode() {
    return this.nodeBooleanQuery.hashCode()
      ^ slop
      ^ (inOrder ? 0x99AFD3BD : 0);
  }

}
