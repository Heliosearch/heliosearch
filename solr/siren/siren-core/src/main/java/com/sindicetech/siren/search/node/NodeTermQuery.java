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

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import com.sindicetech.siren.index.DocsNodesAndPositionsEnum;

import java.io.IOException;
import java.util.Set;

/**
 * A {@link DatatypedNodeQuery} that matches nodes containing a term.
 *
 * <p>
 *
 * Provides an interface to iterate over the candidate documents and nodes
 * containing the term. This may be combined with other terms with a
 * {@link NodeBooleanQuery}.
 */
public class NodeTermQuery extends DatatypedNodeQuery {

  private final Term term;
  private final int docFreq;
  private final TermContext perReaderTermState;

  protected class NodeTermWeight extends Weight {

    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final TermContext termStates;

    public NodeTermWeight(final IndexSearcher searcher, final TermContext termStates)
    throws IOException {
      assert termStates != null : "TermContext must not be null";
      this.termStates = termStates;
      this.similarity = searcher.getSimilarity();
      this.stats = similarity.computeWeight(
        NodeTermQuery.this.getBoost(),
        searcher.collectionStatistics(term.field()),
        searcher.termStatistics(term, termStates));
    }

    @Override
    public String toString() {
      return "weight(" + NodeTermQuery.this + ")";
    }

    @Override
    public Query getQuery() {
      return NodeTermQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(final float queryNorm, final float topLevelBoost) {
      stats.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Scorer scorer(final AtomicReaderContext context, final Bits acceptDocs)
    throws IOException {
      assert termStates.topReaderContext == ReaderUtil.getTopLevelContext(context) : "The top-reader used to create " +
              "Weight (" + termStates.topReaderContext + ") is not the same as the current reader's top-reader (" +
              ReaderUtil.getTopLevelContext(context);
      final TermsEnum termsEnum = this.getTermsEnum(context);
      if (termsEnum == null) {
        return null;
      }

      final DocsAndPositionsEnum docsEnum = termsEnum.docsAndPositions(acceptDocs, null);
      final DocsNodesAndPositionsEnum sirenDocsEnum = NodeTermQuery.this.getDocsNodesAndPositionsEnum(docsEnum);
      return new NodeTermScorer(this, sirenDocsEnum, similarity.simScorer(stats, context));
    }

    /**
     * Returns a {@link TermsEnum} positioned at this weights Term or null if
     * the term does not exist in the given context
     */
    TermsEnum getTermsEnum(final AtomicReaderContext context) throws IOException {
      final TermState state = termStates.get(context.ord);
      if (state == null) { // term is not present in that reader
        assert this.termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
        return null;
      }
      final TermsEnum termsEnum = context.reader().terms(term.field()).iterator(null);
      termsEnum.seekExact(term.bytes(), state);
      return termsEnum;
    }

    private boolean termNotInReader(final AtomicReader reader, final Term term) throws IOException {
      // only called from assert
      return reader.docFreq(term) == 0;
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
      final NodeScorer scorer = (NodeScorer) this.scorer(context, context.reader().getLiveDocs());

      if (scorer != null) {
        if (scorer.skipToCandidate(doc) && scorer.doc() == doc) {
          final Similarity.SimScorer docScorer = similarity.simScorer(stats, context);
          final ComplexExplanation result = new ComplexExplanation();
          float sum = 0;
          result.setDescription("weight("+this.getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], sum of:");
          while (scorer.nextNode()) {
            final ComplexExplanation nodeMatch = new ComplexExplanation();
            nodeMatch.setDescription("in "+scorer.node()+"), result of:");
            final float freq = scorer.freqInNode();
            final Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "termFreq=" + freq));
            nodeMatch.setValue(scoreExplanation.getValue());
            sum += scoreExplanation.getValue();
            nodeMatch.setMatch(true);
            nodeMatch.addDetail(scoreExplanation);
            result.addDetail(nodeMatch);
          }
          result.setValue(sum);
          result.setMatch(true);
          return result;
        }
      }
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }

  }

  /** Constructs a query for the term <code>t</code>. */
  public NodeTermQuery(final Term t) {
    this(t, -1);
  }

  /** Expert: constructs a TermQuery that will use the
   *  provided docFreq instead of looking up the docFreq
   *  against the searcher. */
  public NodeTermQuery(final Term t, final int docFreq) {
    term = t;
    this.docFreq = docFreq;
    perReaderTermState = null;
  }

  /** Expert: constructs a TermQuery that will use the
   *  provided docFreq instead of looking up the docFreq
   *  against the searcher. */
  public NodeTermQuery(final Term t, final TermContext states) {
    assert states != null;
    term = t;
    docFreq = states.docFreq();
    perReaderTermState = states;
  }

  /** Returns the term of this query. */
  public Term getTerm() {
    return term;
  }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    final IndexReaderContext context = searcher.getTopReaderContext();
    final TermContext termState;
    if (perReaderTermState == null || perReaderTermState.topReaderContext != context) {
      // make TermQuery single-pass if we don't have a PRTS or if the context differs!
      termState = TermContext.build(context, term); // cache term lookups!
    } else {
     // PRTS was pre-build for this IS
     termState = this.perReaderTermState;
    }

    // we must not ignore the given docFreq - if set use the given value (lie)
    if (docFreq != -1)
      termState.setDocFreq(docFreq);

    return new NodeTermWeight(searcher, termState);
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    terms.add(this.getTerm());
  }

  /**
   * Prints a user-readable version of this query.
   * <p>
   * The term is wrapped in simple quotes, so that any special characters it
   * may contains are disabled. See ProtectedQueryNode in siren-qparser.
   */
  @Override
  public String toString(final String field) {
    final StringBuilder builder = new StringBuilder();
    final CharSequence text = term.text();
    if (text.length() != 0) {
      builder.append("'").append(text).append("'");
    }
    builder.append(ToStringUtils.boost(this.getBoost()));
    return this.wrapToStringWithDatatype(builder).toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof NodeTermQuery)) return false;
    final NodeTermQuery other = (NodeTermQuery) o;
    return (this.getBoost() == other.getBoost()) &&
            this.term.equals(other.term) &&
            this.levelConstraint == other.levelConstraint &&
            this.lowerBound == other.lowerBound &&
            this.upperBound == other.upperBound &&
            StringUtils.equals(this.datatype, other.datatype);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return Float.floatToIntBits(this.getBoost())
      ^ term.hashCode()
      ^ levelConstraint
      ^ upperBound
      ^ lowerBound;
  }

}
