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

package com.sindicetech.siren.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.search.node.*;
import com.sindicetech.siren.search.node.MultiNodeTermQuery.RewriteMethod;
import com.sindicetech.siren.search.node.NodeBooleanClause.Occur;
import com.sindicetech.siren.search.node.TwigQuery.EmptyRootQuery;
import com.sindicetech.siren.search.spans.BooleanSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.search.spans.TermSpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;
import com.sindicetech.siren.util.XSDDatatype;

import java.io.IOException;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public abstract class AbstractTestSirenScorer extends BasicSirenTestCase {

  public static LuceneProxyNodeQuery dq(final NodeQuery nq) {
    return new LuceneProxyNodeQuery(nq);
  }

  protected NodeScorer getScorer(final NodeQueryBuilder builder)
  throws IOException {
    return (NodeScorer) this.getScorer(builder.getQuery());
  }

  protected Scorer getScorer(final Query query) throws IOException {
    final Weight weight = searcher.createNormalizedWeight(query);
    assertTrue(searcher.getTopReaderContext() instanceof AtomicReaderContext);
    final AtomicReaderContext context = (AtomicReaderContext) searcher.getTopReaderContext();
    Scorer scorer = weight.scorer(context, context.reader().getLiveDocs());
    return scorer;
  }

  public static abstract class QueryBuilder {

    public abstract Query getQuery();

  }

  public static abstract class NodeQueryBuilder extends QueryBuilder {

    public NodeQueryBuilder bound(final int lowerBound, final int upperBound) {
      ((NodeQuery) this.getQuery()).setNodeConstraint(lowerBound, upperBound);
      return this;
    }

    public NodeQueryBuilder level(final int level) {
      ((NodeQuery) this.getQuery()).setLevelConstraint(level);
      return this;
    }

    public abstract Query getLuceneProxyQuery();

    /**
     * Should be implemented only by {@link com.sindicetech.siren.search.node.DatatypedNodeQuery} builders
     */
    public NodeQueryBuilder setDatatype(final String datatype) {
      throw new UnsupportedOperationException();
    }

  }

  public static class NodeNumericRangeQueryBuilder extends NodeQueryBuilder {

    protected final NodeNumericRangeQuery<? extends Number> nmq;

    public NodeNumericRangeQueryBuilder setRewriteMethod(final RewriteMethod method) {
      nmq.setRewriteMethod(method);
      return this;
    }

    private NodeNumericRangeQueryBuilder(final String field,
                                         final int precisionStep,
                                         final Integer min,
                                         final Integer max,
                                         final boolean minInclusive,
                                         final boolean maxInclusive) {
      nmq = NodeNumericRangeQuery
      .newIntRange(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    private NodeNumericRangeQueryBuilder(final String field,
                                         final int precisionStep,
                                         final Float min,
                                         final Float max,
                                         final boolean minInclusive,
                                         final boolean maxInclusive) {
      nmq = NodeNumericRangeQuery
      .newFloatRange(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    private NodeNumericRangeQueryBuilder(final String field,
                                         final int precisionStep,
                                         final Double min,
                                         final Double max,
                                         final boolean minInclusive,
                                         final boolean maxInclusive) {
      nmq = NodeNumericRangeQuery
      .newDoubleRange(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    private NodeNumericRangeQueryBuilder(final String field,
                                         final int precisionStep,
                                         final Long min,
                                         final Long max,
                                         final boolean minInclusive,
                                         final boolean maxInclusive) {
      nmq = NodeNumericRangeQuery
      .newLongRange(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    public static NodeNumericRangeQueryBuilder nmqInt(final String field,
                                                      final int precisionStep,
                                                      final Integer min,
                                                      final Integer max,
                                                      final boolean minInclusive,
                                                      final boolean maxInclusive) {
      return new NodeNumericRangeQueryBuilder(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    public static NodeNumericRangeQueryBuilder nmqFloat(final String field,
                                                        final int precisionStep,
                                                        final Float min,
                                                        final Float max,
                                                        final boolean minInclusive,
                                                        final boolean maxInclusive) {
      return new NodeNumericRangeQueryBuilder(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    public static NodeNumericRangeQueryBuilder nmqDouble(final String field,
                                                         final int precisionStep,
                                                         final Double min,
                                                         final Double max,
                                                         final boolean minInclusive,
                                                         final boolean maxInclusive) {
      return new NodeNumericRangeQueryBuilder(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    public static NodeNumericRangeQueryBuilder nmqLong(final String field,
                                                       final int precisionStep,
                                                       final Long min,
                                                       final Long max,
                                                       final boolean minInclusive,
                                                       final boolean maxInclusive) {
      return new NodeNumericRangeQueryBuilder(field, precisionStep, min, max, minInclusive, maxInclusive);
    }

    @Override
    public NodeQuery getQuery() {
      return nmq;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(nmq);
    }

    @Override
    public NodeQueryBuilder setDatatype(final String datatype) {
      nmq.setDatatype(datatype);
      return this;
    }

  }

  public static class NodeTermQueryBuilder extends NodeQueryBuilder {

    protected final NodeTermQuery ntq;

    private NodeTermQueryBuilder(final String fieldName, final String term) {
      final Term t = new Term(fieldName, term);
      ntq = new NodeTermQuery(t);
      // Add default datatype
      ntq.setDatatype(XSDDatatype.XSD_STRING);
    }

    public static NodeTermQueryBuilder ntq(final String term) {
      return new NodeTermQueryBuilder(DEFAULT_TEST_FIELD, term);
    }

    @Override
    public NodeQuery getQuery() {
      return ntq;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(ntq);
    }

    @Override
    public NodeQueryBuilder setDatatype(final String datatype) {
      ntq.setDatatype(datatype);
      return this;
    }

  }

  public static class TermSpanQueryBuilder extends NodeQueryBuilder {

    protected final TermSpanQuery tsq;

    private TermSpanQueryBuilder(final String fieldName, final String term) {
      final Term t = new Term(fieldName, term);
      tsq = new TermSpanQuery(t);
      // Add default datatype
      tsq.setDatatype(XSDDatatype.XSD_STRING);
    }

    public static TermSpanQueryBuilder tsq(final String term) {
      return new TermSpanQueryBuilder(DEFAULT_TEST_FIELD, term);
    }

    @Override
    public SpanQuery getQuery() {
      return tsq;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(tsq);
    }

    @Override
    public NodeQueryBuilder setDatatype(final String datatype) {
      tsq.setDatatype(datatype);
      return this;
    }

  }

  public static class NodePhraseQueryBuilder extends NodeQueryBuilder {

    protected final NodePhraseQuery npq;

    private NodePhraseQueryBuilder(final String fieldName, final String[] terms) {
      npq = new NodePhraseQuery();
      for (int i = 0; i < terms.length; i++) {
        if (terms[i].isEmpty()) { // if empty string, skip it
          continue;
        }
        final Term t = new Term(fieldName, terms[i]);
        npq.add(t, i);
      }
      // Add default datatype
      npq.setDatatype(XSDDatatype.XSD_STRING);
    }

    /**
     * If term is equal to an empty string, this is considered as a position
     * gap.
     */
    public static NodePhraseQueryBuilder npq(final String ... terms) {
      return npq(DEFAULT_TEST_FIELD, terms);
    }

    /**
     * If term is equal to an empty string, this is considered as a position
     * gap.
     * The field value is passed as an argument
     */
    public static NodePhraseQueryBuilder npq(final String field, final String[] terms) {
      return new NodePhraseQueryBuilder(field, terms);
    }

    @Override
    public NodeQuery getQuery() {
      return npq;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(npq);
    }


    @Override
    public NodeQueryBuilder setDatatype(final String datatype) {
      npq.setDatatype(datatype);
      return this;
    }

  }

  public static class BooleanClauseBuilder {

    public static BooleanBag must(final QueryBuilder... builders) {
      final Query[] queries = new Query[builders.length];
      for (int i = 0; i < builders.length; i++) {
        queries[i] = builders[i].getQuery();
      }
      return BooleanBag.must(queries);
    }

    public static BooleanBag must(final String term) {
      return BooleanBag.must(ntq(term).ntq);
    }

    public static BooleanBag must(final String... terms) {
      final Query[] queries = new Query[terms.length];
      for (int i = 0; i < terms.length; i++) {
        queries[i] = ntq(terms[i]).ntq;
      }
      return BooleanBag.must(queries);
    }

    public static BooleanBag should(final QueryBuilder... builders) {
      final Query[] queries = new Query[builders.length];
      for (int i = 0; i < builders.length; i++) {
        queries[i] = builders[i].getQuery();
      }
      return BooleanBag.should(queries);
    }

    public static BooleanBag should(final String term) {
      return BooleanBag.should(ntq(term).ntq);
    }

    public static BooleanBag should(final String ... terms) {
      final Query[] queries = new Query[terms.length];
      for (int i = 0; i < terms.length; i++) {
        queries[i] = ntq(terms[i]).ntq;
      }
      return BooleanBag.should(queries);
    }

    public static BooleanBag not(final QueryBuilder... builders) {
      final Query[] queries = new Query[builders.length];
      for (int i = 0; i < builders.length; i++) {
        queries[i] = builders[i].getQuery();
      }
      return BooleanBag.not(queries);
    }

    public static BooleanBag not(final String term) {
      return BooleanBag.not(ntq(term).ntq);
    }

    public static BooleanBag not(final String ... terms) {
      final Query[] queries = new Query[terms.length];
      for (int i = 0; i < terms.length; i++) {
        queries[i] = ntq(terms[i]).ntq;
      }
      return BooleanBag.not(queries);
    }

  }

  public static class NodeBooleanQueryBuilder extends NodeQueryBuilder {

    protected NodeBooleanQuery nbq;

    private NodeBooleanQueryBuilder(final BooleanBag[] clauses) {
      nbq = new NodeBooleanQuery();
      for (final BooleanBag bag : clauses) {
        for (final NodeBooleanClause clause : bag.toNodeBooleanClauses()) {
          nbq.add(clause);
        }
      }
    }

    public static NodeBooleanQueryBuilder nbq(final BooleanBag ... clauses) {
      return new NodeBooleanQueryBuilder(clauses);
    }

    @Override
    public NodeQuery getQuery() {
      return nbq;
    }

    @Override
    public NodeBooleanQueryBuilder bound(final int lowerBound, final int upperBound) {
      return (NodeBooleanQueryBuilder) super.bound(lowerBound, upperBound);
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(nbq);
    }

  }

  public static class BooleanSpanQueryBuilder extends NodeQueryBuilder {

    protected BooleanSpanQuery bsq;

    private BooleanSpanQueryBuilder(final BooleanBag[] clauses) {
      bsq = new BooleanSpanQuery(0, false);
      for (final BooleanBag bag : clauses) {
        for (final NodeBooleanClause clause : bag.toNodeBooleanClauses()) {
          bsq.add((SpanQuery) clause.getQuery(), clause.getOccur());
        }
      }
    }

    public static BooleanSpanQueryBuilder bsq(final BooleanBag ... clauses) {
      return new BooleanSpanQueryBuilder(clauses);
    }

    @Override
    public NodeQuery getQuery() {
      return bsq;
    }

    @Override
    public BooleanSpanQueryBuilder bound(final int lowerBound, final int upperBound) {
      return (BooleanSpanQueryBuilder) super.bound(lowerBound, upperBound);
    }

    public BooleanSpanQueryBuilder slop(final int slop) {
      BooleanSpanQuery bsq = new BooleanSpanQuery(slop, this.bsq.isInOrder());
      for (final NodeBooleanClause clause : this.bsq.getClauses()) {
        bsq.add((SpanQuery) clause.getQuery(), clause.getOccur());
      }
      this.bsq = bsq;
      return this;
    }

    public BooleanSpanQueryBuilder inOrder(final boolean inOrder) {
      BooleanSpanQuery bsq = new BooleanSpanQuery(this.bsq.getSlop(), inOrder);
      for (final NodeBooleanClause clause : this.bsq.getClauses()) {
        bsq.add((SpanQuery) clause.getQuery(), clause.getOccur());
      }
      this.bsq = bsq;
      return this;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(bsq);
    }

  }

  public static class BooleanQueryBuilder extends QueryBuilder {

    protected BooleanQuery bq;

    private BooleanQueryBuilder(final BooleanBag[] clauses) {
      bq = new BooleanQuery();
      for (final BooleanBag bag : clauses) {
        for (final BooleanClause clause : bag.toBooleanClauses()) {
          bq.add(clause);
        }
      }
    }

    public static BooleanQueryBuilder bq(final BooleanBag... clauses) {
      return new BooleanQueryBuilder(clauses);
    }

    @Override
    public Query getQuery() {
      return bq;
    }

  }

  public static class TwigQueryBuilder extends NodeQueryBuilder {

    protected TwigQuery twq;

    private TwigQueryBuilder(final int rootLevel, final NodeQueryBuilder builder) {
      twq = new TwigQuery(rootLevel);
      twq.addRoot((NodeQuery) builder.getQuery());
    }

    private TwigQueryBuilder(final int rootLevel) {
      twq = new TwigQuery(rootLevel);
    }

    public static TwigQueryBuilder twq(final int rootLevel, final BooleanBag ... clauses) {
      return new TwigQueryBuilder(rootLevel, NodeBooleanQueryBuilder.nbq(clauses));
    }

    public static TwigQueryBuilder twq(final int rootLevel) {
      return new TwigQueryBuilder(rootLevel);
    }

    public TwigQueryBuilder root(final NodeQueryBuilder root) {
      if (!(twq.getRoot() instanceof EmptyRootQuery)) {
        throw new IllegalArgumentException("The root is already set: " + twq.getRoot());
      }
      twq.addRoot((NodeQuery) root.getQuery());
      return this;
    }

    public TwigQueryBuilder with(final NodeQueryBuilder nq) {
      twq.addChild((NodeQuery) nq.getQuery(), Occur.MUST);
      return this;
    }

    public TwigQueryBuilder without(final NodeQueryBuilder nq) {
      twq.addChild((NodeQuery) nq.getQuery(), Occur.MUST_NOT);
      return this;
    }

    public TwigQueryBuilder optional(final NodeQueryBuilder nq) {
      twq.addChild((NodeQuery) nq.getQuery(), Occur.SHOULD);
      return this;
    }

    public TwigQueryBuilder with(final TwigChildBuilder child) {
      twq.addChild(child.nbq, Occur.MUST);
      return this;
    }

    public TwigQueryBuilder without(final TwigChildBuilder child) {
      twq.addChild(child.nbq, Occur.MUST_NOT);
      return this;
    }

    public TwigQueryBuilder optional(final TwigChildBuilder child) {
      twq.addChild(child.nbq, Occur.SHOULD);
      return this;
    }

    public TwigQueryBuilder with(final TwigDescendantBuilder desc) {
      twq.addDescendant(desc.level, desc.nbq, Occur.MUST);
      return this;
    }

    public TwigQueryBuilder without(final TwigDescendantBuilder desc) {
      twq.addDescendant(desc.level, desc.nbq, Occur.MUST_NOT);
      return this;
    }

    public TwigQueryBuilder optional(final TwigDescendantBuilder desc) {
      twq.addDescendant(desc.level, desc.nbq, Occur.SHOULD);
      return this;
    }

    @Override
    public NodeQuery getQuery() {
      return twq;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(twq);
    }

  }

  public static class TwigChildBuilder {

    NodeBooleanQuery nbq;

    private TwigChildBuilder(final BooleanBag[] clauses) {
      nbq = NodeBooleanQueryBuilder.nbq(clauses).nbq;
    }

    public static TwigChildBuilder child(final BooleanBag ... clauses) {
      return new TwigChildBuilder(clauses);
    }

  }

  public static class TwigDescendantBuilder {

    int level;
    NodeBooleanQuery nbq;

    private TwigDescendantBuilder(final int level, final BooleanBag[] clauses) {
      this.level = level;
      nbq = NodeBooleanQueryBuilder.nbq(clauses).nbq;
    }

    public static TwigDescendantBuilder desc(final int level, final BooleanBag ... clauses) {
      return new TwigDescendantBuilder(level, clauses);
    }

  }

  public static class TupleQueryBuilder extends NodeQueryBuilder {

    protected TupleQuery tq;

    private TupleQueryBuilder() {
      tq = new TupleQuery(true);
    }

    private TupleQueryBuilder(final int rootLevel) {
      tq = new TupleQuery(rootLevel, true);
    }

    public static TupleQueryBuilder tuple() {
      return new TupleQueryBuilder();
    }

    public static TupleQueryBuilder tuple(final int rootLevel) {
      return new TupleQueryBuilder(rootLevel);
    }

    public TupleQueryBuilder with(final NodeBooleanQueryBuilder ... clauses) {
      for (final NodeBooleanQueryBuilder clause : clauses) {
        tq.add(clause.nbq, Occur.MUST);
      }
      return this;
    }

    public TupleQueryBuilder without(final NodeBooleanQueryBuilder ... clauses) {
      for (final NodeBooleanQueryBuilder clause : clauses) {
        tq.add(clause.nbq, Occur.MUST_NOT);
      }
      return this;
    }

    public TupleQueryBuilder optional(final NodeBooleanQueryBuilder ... clauses) {
      for (final NodeBooleanQueryBuilder clause : clauses) {
        tq.add(clause.nbq, Occur.SHOULD);
      }
      return this;
    }

    @Override
    public NodeQuery getQuery() {
      return tq;
    }

    @Override
    public Query getLuceneProxyQuery() {
      return new LuceneProxyNodeQuery(tq);
    }

  }

  /**
   * Assert if a scorer reaches end of stream, and check if sentinel values are
   * set.
   */
  public static void assertEndOfStream(final NodeScorer scorer) throws IOException {
    assertFalse(scorer.nextCandidateDocument());
    assertEquals(DocsAndNodesIterator.NO_MORE_DOC, scorer.doc());
    assertFalse(scorer.nextNode());
    assertEquals(DocsAndNodesIterator.NO_MORE_NOD, scorer.node());
  }

}
