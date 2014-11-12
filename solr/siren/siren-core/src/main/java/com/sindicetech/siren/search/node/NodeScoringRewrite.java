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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoringRewrite;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.RamUsageEstimator;

import com.sindicetech.siren.search.node.MultiNodeTermQuery.RewriteMethod;
import com.sindicetech.siren.search.node.NodeBooleanClause.Occur;

/**
 * Base rewrite method that translates each term into a query, and keeps
 * the scores as computed by the query.
 *
 * <p>
 *
 * Code taken from {@link ScoringRewrite} and adapted for SIREn.
 */
public abstract class NodeScoringRewrite<Q extends Query> extends NodeTermCollectingRewrite<Q> {

  /**
   * A rewrite method that first translates each term into
   * {@link NodeBooleanClause.Occur#SHOULD} clause in a
   * {@link NodeBooleanQuery}, and keeps the scores as computed by the
   * query.  Note that typically such scores are
   * meaningless to the user, and require non-trivial CPU
   * to compute, so it's almost always better to use {@link
   * MultiNodeTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} instead.
   *
   * <p><b>NOTE</b>: This rewrite method will hit {@link
   * NodeBooleanQuery.TooManyClauses} if the number of terms
   * exceeds {@link NodeBooleanQuery#getMaxClauseCount}.
   *
   * @see #setRewriteMethod
   **/
  public final static NodeScoringRewrite<NodeBooleanQuery> SCORING_BOOLEAN_QUERY_REWRITE = new NodeScoringRewrite<NodeBooleanQuery>() {

    @Override
    protected NodeBooleanQuery getTopLevelQuery(final NodeQuery query) {
      NodeBooleanQuery q = new NodeBooleanQuery();
      // set level and node constraints
      q.setLevelConstraint(query.getLevelConstraint());
      q.setNodeConstraint(query.getNodeConstraint()[0], query.getNodeConstraint()[1]);
      // set ancestor
      q.setAncestorPointer(query.ancestor);
      return q;
    }

    @Override
    protected void addClause(final NodeBooleanQuery topLevel, final Term term,
                             final int docCount, final float boost,
                             final TermContext states) {
      final NodeTermQuery tq = new NodeTermQuery(term, states);
      tq.setBoost(boost);
      topLevel.add(tq, Occur.SHOULD);
    }

    @Override
    protected void checkMaxClauseCount(final int count) {
      if (count > BooleanQuery.getMaxClauseCount())
        throw new BooleanQuery.TooManyClauses();
    }

  };

  /**
   * Like {@link #SCORING_BOOLEAN_QUERY_REWRITE} except
   * scores are not computed.  Instead, each matching
   * document receives a constant score equal to the
   * query's boost.
   *
   * <p><b>NOTE</b>: This rewrite method will hit {@link
   * NodeBooleanQuery.TooManyClauses} if the number of terms
   * exceeds {@link NodeBooleanQuery#getMaxClauseCount}.
   *
   * @see #setRewriteMethod
   **/
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = new RewriteMethod() {

    @Override
    public Query rewrite(final IndexReader reader, final MultiNodeTermQuery query) throws IOException {
      final NodeBooleanQuery bq = SCORING_BOOLEAN_QUERY_REWRITE.rewrite(reader, query);
      // TODO: if empty boolean query return NullQuery?
      if (bq.clauses().isEmpty()) {
        return bq;
      }
      // strip the scores off
      final NodeConstantScoreQuery result = new NodeConstantScoreQuery(bq);
      result.setBoost(query.getBoost());
      return result;
    }

  };

  /**
   * This method is called after every new term to check if the number of max clauses
   * (e.g. in NodeBooleanQuery) is not exceeded. Throws the corresponding
   * {@link RuntimeException}.
   */
  protected abstract void checkMaxClauseCount(int count) throws IOException;

  @Override
  public Q rewrite(final IndexReader reader, final MultiNodeTermQuery query)
  throws IOException {
    final Q result = this.getTopLevelQuery(query);
    final ParallelArraysTermCollector col = new ParallelArraysTermCollector();
    this.collectTerms(reader, query, col);

    final int size = col.terms.size();
    if (size > 0) {
      final int sort[] = col.terms.sort(col.termsEnum.getComparator());
      final float[] boost = col.array.boost;
      final TermContext[] termStates = col.array.termState;
      for (int i = 0; i < size; i++) {
        final int pos = sort[i];
        final Term term = new Term(query.getField(), col.terms.get(pos, new BytesRef()));
        assert reader.docFreq(term) == termStates[pos].docFreq();
        this.addClause(result, term, termStates[pos].docFreq(), query.getBoost() * boost[pos], termStates[pos]);
      }
    }
    return result;
  }

  final class ParallelArraysTermCollector extends TermCollector {
    final TermFreqBoostByteStart array = new TermFreqBoostByteStart(16);
    final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectAllocator()), 16, array);
    TermsEnum termsEnum;

    private BoostAttribute boostAtt;

    @Override
    public void setNextEnum(final TermsEnum termsEnum) throws IOException {
      this.termsEnum = termsEnum;
      this.boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
    }

    @Override
    public boolean collect(final BytesRef bytes) throws IOException {
      final int e = terms.add(bytes);
      final TermState state = termsEnum.termState();
      assert state != null;
      if (e < 0 ) {
        // duplicate term: update docFreq
        final int pos = (-e)-1;
        array.termState[pos].register(state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        assert array.boost[pos] == boostAtt.getBoost() : "boost should be equal in all segment TermsEnums";
      } else {
        // new entry: we populate the entry initially
        array.boost[e] = boostAtt.getBoost();
        array.termState[e] = new TermContext(topReaderContext, state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        NodeScoringRewrite.this.checkMaxClauseCount(terms.size());
      }
      return true;
    }
  }

  /** Special implementation of BytesStartArray that keeps parallel arrays for boost and docFreq */
  static final class TermFreqBoostByteStart extends DirectBytesStartArray  {
    float[] boost;
    TermContext[] termState;

    public TermFreqBoostByteStart(final int initSize) {
      super(initSize);
    }

    @Override
    public int[] init() {
      final int[] ord = super.init();
      boost = new float[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_FLOAT)];
      termState = new TermContext[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      assert termState.length >= ord.length && boost.length >= ord.length;
      return ord;
    }

    @Override
    public int[] grow() {
      final int[] ord = super.grow();
      boost = ArrayUtil.grow(boost, ord.length);
      if (termState.length < ord.length) {
        final TermContext[] tmpTermState = new TermContext[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(termState, 0, tmpTermState, 0, termState.length);
        termState = tmpTermState;
      }
      assert termState.length >= ord.length && boost.length >= ord.length;
      return ord;
    }

    @Override
    public int[] clear() {
     boost = null;
     termState = null;
     return super.clear();
    }

  }

}
