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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/**
 * Class that act as a bridge between the SIREn query API and the Lucene query
 * API.
 */
public class LuceneProxyNodeQuery extends Query {

  private final NodeQuery nodeQuery;

  protected class LuceneProxyNodeWeight extends Weight {

    private final Weight weight;

    public LuceneProxyNodeWeight(final Weight weight) {
      this.weight = weight;
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc)
    throws IOException {
      final LuceneProxyNodeScorer dScorer = (LuceneProxyNodeScorer) this.scorer(context, context.reader().getLiveDocs());

      if (dScorer != null) {
        if (dScorer.advance(doc) != DocIdSetIterator.NO_MORE_DOCS && dScorer.docID() == doc) {
          final Explanation exp = dScorer.getWeight().explain(context, doc);
          exp.setValue(dScorer.score());
          return exp;
        }
      }
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }

    @Override
    public Query getQuery() {
      return nodeQuery;
    }

    @Override
    public float getValueForNormalization()
    throws IOException {
      return weight.getValueForNormalization();
    }

    @Override
    public void normalize(final float norm, final float topLevelBoost) {
      weight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs)
    throws IOException {
      final NodeScorer nodeScorer = (NodeScorer) weight.scorer(context, acceptDocs);
      return nodeScorer == null ? null // no match
                                : new LuceneProxyNodeScorer(nodeScorer);
    }

  }

  public LuceneProxyNodeQuery(final NodeQuery nq) {
    this.nodeQuery = nq;
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher)
  throws IOException {
    return new LuceneProxyNodeWeight(nodeQuery.createWeight(searcher));
  }

  @Override
  public Query rewrite(final IndexReader reader)
  throws IOException {
    final Query rewroteQuery = nodeQuery.rewrite(reader);

    if (nodeQuery == rewroteQuery) {
      return this;
    }
    final LuceneProxyNodeQuery q = new LuceneProxyNodeQuery((NodeQuery) rewroteQuery);
    q.setBoost(nodeQuery.getBoost());
    return q;
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    nodeQuery.extractTerms(terms);
  }

  @Override
  public String toString(final String field) {
    final StringBuffer buffer = new StringBuffer();
    final boolean withParen = (this.getBoost() != 1.0) ||
                              (nodeQuery instanceof TwigQuery);
    if (withParen) {
      buffer.append('(');
    }
    buffer.append(nodeQuery.toString(field));
    if (withParen) {
      buffer.append(')').append(ToStringUtils.boost(this.getBoost()));
    }
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof LuceneProxyNodeQuery)) return false;
    final LuceneProxyNodeQuery other = (LuceneProxyNodeQuery) o;
    return this.nodeQuery.equals(other.nodeQuery);
  }

  @Override
  public Query clone() {
    NodeQuery nodeClone = (NodeQuery) nodeQuery.clone();
    LuceneProxyNodeQuery proxyClone = new LuceneProxyNodeQuery(nodeClone);
    return proxyClone;
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(this.getBoost()) ^ nodeQuery.hashCode();
  }

  public NodeQuery getNodeQuery() {
    return nodeQuery;
  }

  @Override
  public void setBoost(final float b) {
    nodeQuery.setBoost(b);
  }

}
