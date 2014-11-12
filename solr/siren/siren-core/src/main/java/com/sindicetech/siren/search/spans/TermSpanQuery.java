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

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import com.sindicetech.siren.index.DocsNodesAndPositionsEnum;
import com.sindicetech.siren.search.node.NodeScorer;

import java.io.IOException;
import java.util.Set;

/**
 * Base class for term-based span queries.
 * <p>
 * A term-based span query matches spans of terms. The positions of the span is relative to the position of a
 * {@link org.apache.lucene.index.Term}.
 * <p>
 * Code taken from {@link org.apache.lucene.search.spans.SpanTermQuery} and adapted for the Siren use case.
 */
public class TermSpanQuery extends DatatypedSpanQuery {

  protected Term term;

  /**
   * TODO: Duplicate code with {@link com.sindicetech.siren.search.node.NodeTermQuery.NodeTermWeight}
   */
  protected class TermSpanWeight extends Weight {

    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final TermContext termStates;

    public TermSpanWeight(final IndexSearcher searcher, final TermContext termStates) throws IOException {
      assert termStates != null : "TermContext must not be null";
      this.termStates = termStates;
      this.similarity = searcher.getSimilarity();
      this.stats = similarity.computeWeight(
        TermSpanQuery.this.getBoost(),
        searcher.collectionStatistics(term.field()),
        searcher.termStatistics(term, termStates));
    }

    @Override
    public String toString() {
      return "weight(" + TermSpanQuery.this + ")";
    }

    @Override
    public Explanation explain(final AtomicReaderContext context, final int doc) throws IOException {
      final NodeScorer scorer = (NodeScorer) this.scorer(context, context.reader().getLiveDocs());

      if (scorer != null) {
        if (scorer.skipToCandidate(doc) && scorer.doc() == doc) {
          final Similarity.SimScorer docScorer = similarity.simScorer(stats, context);
          final ComplexExplanation result = new ComplexExplanation();
          result.setDescription("weight("+this.getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], sum of:");
          float sum = 0;
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

    @Override
    public Query getQuery() {
      return TermSpanQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(final float norm, final float topLevelBoost) {
      stats.normalize(norm, topLevelBoost);
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
      final DocsNodesAndPositionsEnum sirenDocsEnum = TermSpanQuery.this.getDocsNodesAndPositionsEnum(docsEnum);
      TermSpans spans = new TermSpans(sirenDocsEnum, term, similarity.simScorer(stats, context));
      return new SpanScorer(this, spans);
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

  }

  /** Construct a TermSpanQuery matching the named term's spans. */
  public TermSpanQuery(Term term) { this.term = term; }

  /** Return the term whose spans are matched. */
  public Term getTerm() { return term; }

  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    final IndexReaderContext context = searcher.getTopReaderContext();
    final TermContext termState = TermContext.build(context, term);
    return new TermSpanWeight(searcher, termState);
  }

  @Override
  public void extractTerms(final Set<Term> terms) {
    terms.add(term);
  }

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

  @Override
  public int hashCode() {
    return Float.floatToIntBits(this.getBoost())
      ^ term.hashCode()
      ^ levelConstraint
      ^ upperBound
      ^ lowerBound;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof TermSpanQuery)) return false;
    final TermSpanQuery other = (TermSpanQuery) o;
    return (this.getBoost() == other.getBoost()) &&
            this.term.equals(other.term) &&
            this.levelConstraint == other.levelConstraint &&
            this.lowerBound == other.lowerBound &&
            this.upperBound == other.upperBound &&
            StringUtils.equals(this.datatype, other.datatype);
  }

}
