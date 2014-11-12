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
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * Expert: A {@link NodeQuery} that filters nodes and return their ancestors.
 *
 * <p>
 *
 * Internal class that is created by the {@link TwigQuery} during query
 * rewriting.
 */
class AncestorFilterQuery extends NodeQuery {

  private final NodeQuery q;
  private final int ancestorLevel;

  /**
   * Expert: constructs a AncestorFilterQuery that will use the
   * provided {@link NodeQuery} and that will filter matching nodes to return
   * their ancestor node based on the given ancestor level. */
  public AncestorFilterQuery(final NodeQuery q, final int ancestorLevel) {
    this.q = q;
    this.ancestorLevel = ancestorLevel;
    this.setLevelConstraint(ancestorLevel);
  }

  public NodeQuery getQuery() {
    return q;
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    return new AncestorFilterWeight(searcher);
  }

  @Override
  public String toString(final String field) {
    return q.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof AncestorFilterQuery)) {
      return false;
    }
    final AncestorFilterQuery other = (AncestorFilterQuery) o;
    return (this.getBoost() == other.getBoost()) &&
           this.q.equals(other.q) &&
           this.lowerBound == other.lowerBound &&
           this.upperBound == other.upperBound &&
           this.levelConstraint == other.levelConstraint;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Float.floatToIntBits(this.getBoost());
    result = prime * result + q.hashCode();
    result = prime * result + lowerBound;
    result = prime * result + upperBound;
    result = prime * result + levelConstraint;
    return result;
  }

  protected class AncestorFilterWeight extends Weight {

    final Weight weight;

    public AncestorFilterWeight(final IndexSearcher searcher) throws IOException {
      this.weight = q.createWeight(searcher);
    }

    @Override
    public String toString() {
      return "weight(" + AncestorFilterQuery.this + ")";
    }

    @Override
    public Query getQuery() {
      return AncestorFilterQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return weight.getValueForNormalization();
    }

    @Override
    public void normalize(final float queryNorm, final float topLevelBoost) {
      weight.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs)
    throws IOException {
      final NodeScorer scorer = (NodeScorer) weight.scorer(context, acceptDocs);
      if (scorer == null) {
        return null;
      }
      return new AncestorFilterScorer(scorer, ancestorLevel);
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc)
    throws IOException {
      return weight.explain(context, doc);
    }

  }

}
