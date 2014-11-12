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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeScoringRewrite;
import com.sindicetech.siren.search.node.TopNodeTermsRewrite;

import java.io.IOException;

/**
 * Wraps any {@link MultiNodeTermQuery} as a {@link SpanQuery},
 * so it can be nested within other {@link SpanQuery} classes.
 * <p>
 * The query is rewritten by default to a {@link OrSpanQuery} containing
 * the expanded terms, but this can be customized.
 * <p>
 * Example:<pre><code>
 * NodeWildcardQuery wildcard = new NodeWildcardQuery(new Term("field", "bro?n"));
 * SpanQuery spanWildcard = new MultiTermSpanQuery<NodeWildcardQuery>(wildcard);
 * // do something with spanWildcard, such as use it in a TermSpanFirstQuery</code></pre>
 * <p>
 * A port of Lucene's {@link org.apache.lucene.search.spans.SpanMultiTermQueryWrapper}
 */
public class MultiTermSpanQuery <Q extends MultiNodeTermQuery> extends DatatypedSpanQuery {

  protected final Q query;

  /**
   * Create a new MultiTermSpanQuery.
   *
   * @param query Query to wrap.
   * <p>
   * NOTE: This will call {@link MultiNodeTermQuery#setRewriteMethod(MultiNodeTermQuery.RewriteMethod)}
   * on the wrapped <code>query</code>, changing its rewrite method to a suitable one for spans.
   * Be sure to not change the rewrite method on the wrapped query afterwards! Doing so will
   * throw {@link UnsupportedOperationException} on rewriting this query!
   */
  @SuppressWarnings({"rawtypes"})
  public MultiTermSpanQuery(Q query) {
    this.query = query;

    MultiNodeTermQuery.RewriteMethod method = query.getRewriteMethod();
    if (method instanceof TopNodeTermsRewrite) {
      final int pqsize = ((TopNodeTermsRewrite) method).getSize();
      setRewriteMethod(new TopTermsSpanBooleanQueryRewrite(pqsize));
    } else {
      setRewriteMethod(SCORING_SPAN_QUERY_REWRITE);
    }
  }

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

  /**
   * Expert: returns the rewriteMethod
   */
  public final SpanRewriteMethod getRewriteMethod() {
    final MultiNodeTermQuery.RewriteMethod m = query.getRewriteMethod();
    if (!(m instanceof SpanRewriteMethod))
      throw new UnsupportedOperationException("You can only use MultiTermSpanQuery with a suitable SpanRewriteMethod.");
    return (SpanRewriteMethod) m;
  }

  /**
   * Expert: sets the rewrite method. This only makes sense
   * to be a span rewrite method.
   */
  public final void setRewriteMethod(SpanRewriteMethod rewriteMethod) {
    query.setRewriteMethod(rewriteMethod);
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query q = query.rewrite(reader);

    if (!(q instanceof SpanQuery)) {
      throw new UnsupportedOperationException("You can only use MultiTermSpanQuery with a suitable SpanRewriteMethod.");
    }

    if (q != this.query) {
      // transfer constraints
      ((SpanQuery) q).setNodeConstraint(lowerBound, upperBound);
      ((SpanQuery) q).setLevelConstraint(levelConstraint);
      // transfer ancestor pointer
      ((SpanQuery) q).setAncestorPointer(ancestor);
    }

    return q;
  }

  @Override
  public int hashCode() {
    return 31 * query.hashCode();
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    final MultiTermSpanQuery other = (MultiTermSpanQuery) obj;
    return query.equals(other.query);
  }

  /** Abstract class that defines how the query is rewritten. */
  public static abstract class SpanRewriteMethod extends MultiNodeTermQuery.RewriteMethod {
    @Override
    public abstract Query rewrite(IndexReader reader, MultiNodeTermQuery query) throws IOException;
  }

  /**
   * A rewrite method that first translates each term into a SpanTermQuery in a
   * {@link Occur#SHOULD} clause in a BooleanQuery, and keeps the
   * scores as computed by the query.
   *
   * @see #setRewriteMethod
   */
  public final static SpanRewriteMethod SCORING_SPAN_QUERY_REWRITE = new SpanRewriteMethod() {
    private final NodeScoringRewrite<OrSpanQuery> delegate = new NodeScoringRewrite<OrSpanQuery>() {
      @Override
      protected OrSpanQuery getTopLevelQuery(final NodeQuery query) {
        OrSpanQuery q = new OrSpanQuery();
        // set level and node constraints
        q.setLevelConstraint(query.getLevelConstraint());
        q.setNodeConstraint(query.getNodeConstraint()[0], query.getNodeConstraint()[1]);
        // set ancestor
        q.setAncestorPointer(query.getAncestorPointer());
        return q;
      }

      @Override
      protected void checkMaxClauseCount(int count) {
        // we accept all terms as OrSpanQuery has no limits
      }

      @Override
      protected void addClause(OrSpanQuery topLevel, Term term, int docCount, float boost, TermContext states) {
        // TODO: would be nice to not lose term-state here.
        // we could add a hack option to OrSpanQuery, but the hack would only work if this is the top-level Span
        // (if you put this thing in another span query, it would extractTerms/double-seek anyway)
        final TermSpanQuery q = new TermSpanQuery(term);
        q.setBoost(boost);
        topLevel.addClause(q);
      }
    };

    @Override
    public Query rewrite(IndexReader reader, MultiNodeTermQuery query) throws IOException {
      return delegate.rewrite(reader, query);
    }
  };

  /**
   * A rewrite method that first translates each term into a SpanTermQuery in a
   * {@link Occur#SHOULD} clause in a BooleanQuery, and keeps the
   * scores as computed by the query.
   *
   * <p>
   * This rewrite method only uses the top scoring terms so it will not overflow
   * the boolean max clause count.
   *
   * @see #setRewriteMethod
   */
  public static final class TopTermsSpanBooleanQueryRewrite extends SpanRewriteMethod  {
    private final TopNodeTermsRewrite<OrSpanQuery> delegate;

    /**
     * Create a TopTermsSpanBooleanQueryRewrite for
     * at most <code>size</code> terms.
     */
    public TopTermsSpanBooleanQueryRewrite(int size) {
      delegate = new TopNodeTermsRewrite<OrSpanQuery>(size) {
        @Override
        protected int getMaxSize() {
          return Integer.MAX_VALUE;
        }

        @Override
        protected OrSpanQuery getTopLevelQuery(final NodeQuery query) {
          OrSpanQuery q = new OrSpanQuery();
          // set level and node constraints
          q.setLevelConstraint(query.getLevelConstraint());
          q.setNodeConstraint(query.getNodeConstraint()[0], query.getNodeConstraint()[1]);
          // set ancestor
          q.setAncestorPointer(query.getAncestorPointer());
          return q;
        }

        @Override
        protected void addClause(OrSpanQuery topLevel, Term term, int docFreq, float boost, TermContext states) {
          final TermSpanQuery q = new TermSpanQuery(term);
          q.setBoost(boost);
          topLevel.addClause(q);
        }
      };
    }

    /** return the maximum priority queue size */
    public int getSize() {
      return delegate.getSize();
    }

    @Override
    public OrSpanQuery rewrite(IndexReader reader, MultiNodeTermQuery query) throws IOException {
      return delegate.rewrite(reader, query);
    }

    @Override
    public int hashCode() {
      return 31 * delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      final TopTermsSpanBooleanQueryRewrite other = (TopTermsSpanBooleanQueryRewrite) obj;
      return delegate.equals(other.delegate);
    }

  }

}
