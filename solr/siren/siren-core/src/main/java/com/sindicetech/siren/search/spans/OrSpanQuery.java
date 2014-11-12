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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Matches the union of its clauses
 */
public class OrSpanQuery extends SpanQuery implements Cloneable {

  protected List<SpanQuery> clauses;

  protected class OrSpanWeight extends Weight {

    protected Similarity similarity;
    protected ArrayList<Weight> weights;

    public OrSpanWeight(final IndexSearcher searcher) throws IOException {
      weights = new ArrayList<Weight>(clauses.size());
      for (int i = 0; i < clauses.size(); i++) {
        final SpanQuery c = clauses.get(i);

        // pass to child query the node constraints
        c.setNodeConstraint(lowerBound, upperBound);
        c.setLevelConstraint(levelConstraint);

        // transfer ancestor pointer to child
        c.setAncestorPointer(ancestor);

        weights.add(c.createWeight(searcher));
      }
    }

    @Override
    public String toString() {
      return "weight(" + OrSpanQuery.this + ")";
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
      final ComplexExplanation avgExpl = new ComplexExplanation();
      avgExpl.setDescription("sloppy sum of:");
      //  TODO: How to get the sloppy frequency information ?

      float sum = 0.0f;
      boolean fail = false;
      final Iterator<SpanQuery> cIter = clauses.iterator();

      for (final Weight w : weights) {
        final SpanQuery c = cIter.next();
        if (w.scorer(context, context.reader().getLiveDocs()) == null) {
          fail = true;
          final Explanation r = new Explanation(0.0f, "no match on span clause (" + c.toString() + ")");
          avgExpl.addDetail(r);
          continue;
        }
        final Explanation e = w.explain(context, doc);
        if (e.isMatch()) {
          avgExpl.addDetail(e);
          sum += e.getValue();
        }
        else {
          final Explanation r = new Explanation(0.0f, "no match on span clause (" + c.toString() + ")");
          r.addDetail(e);
          avgExpl.addDetail(r);
          fail = true;
        }
      }
      if (fail) {
        avgExpl.setMatch(Boolean.FALSE);
        avgExpl.setValue(0.0f);
        avgExpl.setDescription("Failure to meet condition(s) of span clause(s)");
        return avgExpl;
      }

      avgExpl.setMatch(Boolean.TRUE);
      avgExpl.setValue(sum);
      return avgExpl;
    }

    @Override
    public Query getQuery() {
      return OrSpanQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      float sum = 0.0f;
      for (int i = 0; i < weights.size(); i++) {
        sum += weights.get(i).getValueForNormalization(); // sum sub weights
      }

      // boost each sub-weight
      sum *= OrSpanQuery.this.getBoost() * OrSpanQuery.this.getBoost();

      return sum;
    }

    @Override
    public void normalize(final float norm, float topLevelBoost) {
      // incorporate boost
      topLevelBoost *= OrSpanQuery.this.getBoost();
      for (final Weight w : weights) {
        // normalize all clauses
        w.normalize(norm, topLevelBoost);
      }
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
      final List<Spans> spans = new ArrayList<Spans>();
      for (final Weight w  : weights) {
        final Scorer scorer = w.scorer(context, acceptDocs);
        if (scorer == null) {
          continue; // skip scorer if null since it is optional
        }
        if (!(scorer instanceof SpanScorer)) {
          throw new IllegalArgumentException("SpanScorer expected");
        }
        spans.add(((SpanScorer) scorer).getSpans());
      }

      return new SpanScorer(this, new OrSpans(spans));
    }

  }

  public OrSpanQuery() {
    this(new SpanQuery[] {});
  }

  public OrSpanQuery(final SpanQuery... clauses) {
    // copy clauses array into an ArrayList
    this.clauses = new ArrayList<SpanQuery>(clauses.length);

    for (int i = 0; i < clauses.length; i++) {
      this.addClause(clauses[i]);
    }
  }

  @Override
  public void setLevelConstraint(final int levelConstraint) {
    super.setLevelConstraint(levelConstraint);
    for (SpanQuery clause : clauses) {
      clause.setLevelConstraint(levelConstraint);
    }
  }

  @Override
  public void setNodeConstraint(final int lowerBound, final int upperBound) {
    super.setNodeConstraint(lowerBound, upperBound);
    // keep clauses synchronised
    for (SpanQuery clause : clauses) {
      clause.setNodeConstraint(lowerBound, upperBound);
    }
  }

  @Override
  public void setAncestorPointer(final NodeQuery ancestor) {
    super.setAncestorPointer(ancestor);
    // keep clauses synchronised
    for (SpanQuery clause : clauses) {
      clause.setAncestorPointer(ancestor);
    }
  }

  public final void addClause(SpanQuery clause) {
    this.clauses.add(clause);
    // keep clause synchronised in term of constraint management
    clause.setLevelConstraint(levelConstraint);
    clause.setNodeConstraint(lowerBound, upperBound);
    clause.setAncestorPointer(ancestor);
  }

  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    return new OrSpanWeight(searcher);
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    for (final SpanQuery clause : clauses) {
      clause.extractTerms(terms);
    }
  }

  @Override
  public String toString(final String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanOr([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public OrSpanQuery clone() {
    int sz = clauses.size();
    final OrSpanQuery clone = (OrSpanQuery) super.clone();
    for (int i = 0; i < sz; i++) {
      clone.clauses.set(i, (SpanQuery) clauses.get(i).clone());
    }
    return clone;
  }

  @Override
  public Query rewrite(final IndexReader reader) throws IOException {
    if (clauses.size() == 1) {                      // optimize 1-clause queries
      final SpanQuery c = clauses.get(0);

      // rewrite first
      SpanQuery query = (SpanQuery) c.rewrite(reader);

      if (this.getBoost() != 1.0f) {                // incorporate boost
        if (query == c) {                           // if rewrite was no-op
          query = (SpanQuery) query.clone();    // then clone before boost
        }
        query.setBoost(this.getBoost() * query.getBoost());
      }

      // transfer constraints
      query.setNodeConstraint(lowerBound, upperBound);
      query.setLevelConstraint(levelConstraint);

      // transfer ancestor pointer
      query.setAncestorPointer(ancestor);

      return query;
    }

    OrSpanQuery clone = null;                    // recursively rewrite
    for (int i = 0 ; i < clauses.size(); i++) {
      final SpanQuery c = clauses.get(i);
      final SpanQuery query = (SpanQuery) c.rewrite(reader);
      if (query != c) {                     // clause rewrote: must clone
        if (clone == null) {
          clone = this.clone();
        }

        // transfer constraints
        query.setNodeConstraint(lowerBound, upperBound);
        query.setLevelConstraint(levelConstraint);

        // transfer ancestor pointer
        query.setAncestorPointer(ancestor);

        clone.clauses.set(i, query);
      }
    }
    if (clone != null) {
      return clone;                               // some clauses rewrote
    }
    else {
      return this;                                // no clauses rewrote
    }
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof OrSpanQuery)) return false;
    final OrSpanQuery other = (OrSpanQuery) o;
    return (this.getBoost() == other.getBoost()) &&
            this.clauses.equals(other.clauses) &&
            this.levelConstraint == other.levelConstraint &&
            this.lowerBound == other.lowerBound &&
            this.upperBound == other.upperBound;
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(this.getBoost())
            ^ clauses.hashCode()
            ^ levelConstraint
            ^ upperBound
            ^ lowerBound;
  }

}
