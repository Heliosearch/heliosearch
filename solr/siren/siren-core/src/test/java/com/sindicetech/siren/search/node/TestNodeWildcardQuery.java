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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixTermsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodeConstantScoreQuery;
import com.sindicetech.siren.search.node.NodeFuzzyQuery;
import com.sindicetech.siren.search.node.NodePrefixQuery;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;
import com.sindicetech.siren.search.node.NodeWildcardQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;
import com.sindicetech.siren.util.XSDDatatype;

import java.io.IOException;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.dq;

/**
 * TestSirenWildcardQuery tests the '*' and '?' wildcard characters.
 */
public class TestNodeWildcardQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    final TupleAnalyzer tupleAnalyzer = new TupleAnalyzer(TEST_VERSION_CURRENT,
      new WhitespaceAnalyzer(TEST_VERSION_CURRENT), uriAnalyzer);
    tupleAnalyzer.registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);
    this.setAnalyzer(tupleAnalyzer);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  public void testEquals() {
    final NodeWildcardQuery wq1 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "b*a"));
    final NodeWildcardQuery wq2 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "b*a"));
    final NodeWildcardQuery wq3 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "b*a"));

    // reflexive?
    assertEquals(wq1, wq2);
    assertEquals(wq2, wq1);

    // transitive?
    assertEquals(wq2, wq3);
    assertEquals(wq1, wq3);

    assertFalse(wq1.equals(null));

    final NodeFuzzyQuery fq = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "b*a"));
    assertFalse(wq1.equals(fq));
    assertFalse(fq.equals(wq1));
  }

  /**
   * Tests if the ConstantScore filter rewrite return an exception
   */
  @Test(expected=UnsupportedOperationException.class)
  public void testFilterRewrite() throws IOException {
    this.addDocument("<nowildcard> <nowildcardx>");

    final MultiNodeTermQuery wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "nowildcard"));
    this.assertMatches(searcher, wq, 1);

    wq.setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
    wq.setBoost(0.2F);
    searcher.rewrite(wq);
  }

  /**
   * Tests if a SirenWildcardQuery that has no wildcard in the term is rewritten to a single
   * TermQuery. The boost should be preserved, and the rewrite should return
   * a SirenConstantScoreQuery if the SirenWildcardQuery had a ConstantScore rewriteMethod.
   */
  public void testTermWithoutWildcard() throws IOException {
    this.addDocument("<nowildcard> <nowildcardx>");
    final MultiNodeTermQuery wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "nowildcard"));
    this.assertMatches(searcher, wq, 1);

    wq.setRewriteMethod(MultiNodeTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    wq.setBoost(0.1F);
    Query q = searcher.rewrite(wq);
    assertTrue(q instanceof NodeTermQuery);
    assertEquals(q.getBoost(), wq.getBoost(), 0);

    wq.setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    wq.setBoost(0.3F);
    q = searcher.rewrite(wq);
    assertTrue(q instanceof NodeConstantScoreQuery);
    assertEquals(q.getBoost(), wq.getBoost(), 0.1);

    wq.setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    wq.setBoost(0.4F);
    q = searcher.rewrite(wq);
    assertTrue(q instanceof NodeConstantScoreQuery);
    assertEquals(q.getBoost(), wq.getBoost(), 0.1);
  }

  /**
   * Tests if a SirenWildcardQuery with an empty term is rewritten to an empty
   * SirenBooleanQuery
   */
  public void testEmptyTerm() throws IOException {
    this.addDocument("<nowildcard> <nowildcardx>");

    final MultiNodeTermQuery wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, ""));
    wq.setRewriteMethod(MultiNodeTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    this.assertMatches(searcher, wq, 0);
    final Query q = searcher.rewrite(wq);
    assertTrue(q instanceof NodeBooleanQuery);
    assertEquals(0, ((NodeBooleanQuery) q).clauses().size());
  }

  /**
   * Tests if a SirenWildcardQuery that has only a trailing * in the term is
   * rewritten to a single SirenPrefixQuery. The boost and rewriteMethod should be
   * preserved.
   */
  public void testPrefixTerm() throws IOException {
    this.addDocuments("<prefix>", "<prefixx>");

    MultiNodeTermQuery wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "prefix*"));
    this.assertMatches(searcher, wq, 2);
    final Terms terms = MultiFields.getTerms(searcher.getIndexReader(), DEFAULT_TEST_FIELD);
    assertTrue(wq.getTermsEnum(terms) instanceof PrefixTermsEnum);

    final MultiNodeTermQuery expected = new NodePrefixQuery(new Term(DEFAULT_TEST_FIELD, "prefix"));
    wq.setRewriteMethod(MultiNodeTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    wq.setBoost(0.1F);
    expected.setRewriteMethod(wq.getRewriteMethod());
    expected.setBoost(wq.getBoost());
    assertEquals(searcher.rewrite(expected), searcher.rewrite(wq));

    wq.setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    wq.setBoost(0.3F);
    expected.setRewriteMethod(wq.getRewriteMethod());
    expected.setBoost(wq.getBoost());
    assertEquals(searcher.rewrite(expected), searcher.rewrite(wq));

    wq.setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    wq.setBoost(0.4F);
    expected.setRewriteMethod(wq.getRewriteMethod());
    expected.setBoost(wq.getBoost());
    assertEquals(searcher.rewrite(expected), searcher.rewrite(wq));

    wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "*"));
    this.assertMatches(searcher, wq, 2);
    assertFalse(wq.getTermsEnum(terms) instanceof PrefixTermsEnum);
    assertFalse(wq.getTermsEnum(terms).getClass().getSimpleName().contains("AutomatonTermsEnum"));
  }

  /**
   * Tests Wildcard queries with an asterisk.
   */
  public void testAsterisk() throws IOException {
    this.addDocuments("<metal>", "<metals>");

    final NodeQuery query1 = new NodeTermQuery(new Term(DEFAULT_TEST_FIELD, "metal"));
    final NodeQuery query2 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "metal*"));
    final NodeQuery query3 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "m*tal"));
    final NodeQuery query4 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "m*tal*"));
    final NodeQuery query5 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "m*tals"));

    final NodeBooleanQuery query6 = new NodeBooleanQuery();
    query6.add(query5, NodeBooleanClause.Occur.SHOULD);

    final NodeBooleanQuery query7 = new NodeBooleanQuery();
    query7.add(query3, NodeBooleanClause.Occur.SHOULD);
    query7.add(query5, NodeBooleanClause.Occur.SHOULD);

    // Queries do not automatically lower-case search terms:
    final NodeQuery query8 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "M*tal*"));

    this.assertMatches(searcher, query1, 1);
    this.assertMatches(searcher, query2, 2);
    this.assertMatches(searcher, query3, 1);
    this.assertMatches(searcher, query4, 2);
    this.assertMatches(searcher, query5, 1);
    this.assertMatches(searcher, query6, 1);
    this.assertMatches(searcher, query7, 2);
    this.assertMatches(searcher, query8, 0);
    this.assertMatches(searcher, new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "*tall")), 0);
    this.assertMatches(searcher, new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "*tal")), 1);
    this.assertMatches(searcher, new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "*tal*")), 2);
  }

  /**
   * Tests Wildcard queries with a question mark.
   *
   * @throws IOException if an error occurs
   */
  public void testQuestionmark() throws IOException {
    this.addDocuments("<metal>", "<metals>", "<mXtals>", "<mXtXls>");

    final NodeQuery query1 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "m?tal"));
    final NodeQuery query2 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "metal?"));
    final NodeQuery query3 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "metals?"));
    final NodeQuery query4 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "m?t?ls"));
    final NodeQuery query5 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "M?t?ls"));
    final NodeQuery query6 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "meta??"));

    this.assertMatches(searcher, query1, 1);
    this.assertMatches(searcher, query2, 1);
    this.assertMatches(searcher, query3, 0);
    this.assertMatches(searcher, query4, 3);
    this.assertMatches(searcher, query5, 0);
    this.assertMatches(searcher, query6, 1); // Query: 'meta??' matches 'metals' not 'metal'
  }

  private void assertMatches(final IndexSearcher searcher, final NodeQuery q, final int expectedMatches)
  throws IOException {
    final ScoreDoc[] result = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals(expectedMatches, result.length);
  }

}