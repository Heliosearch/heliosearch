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
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermRangeTermsEnum;
import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;
import com.sindicetech.siren.search.node.NodeTermRangeQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;
import com.sindicetech.siren.util.XSDDatatype;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.dq;

public class TestNodeTermRangeQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    final TupleAnalyzer tupleAnalyzer = new TupleAnalyzer(TEST_VERSION_CURRENT,
      new WhitespaceAnalyzer(TEST_VERSION_CURRENT), uriAnalyzer);
    tupleAnalyzer.registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);
    this.setAnalyzer(tupleAnalyzer);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  public void testExclusive1() throws Exception {
    this.addDocument("</computera>");
    this.addDocument("</computerb>");
    this.addDocument("</computerc>");
    this.addDocument("</computerd>");

    final NodeQuery q = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", false, false);

    final ScoreDoc[] hits = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals("A,B,C,D, only B in range", 1, hits.length);
  }

  public void testExclusive2() throws Exception {
    this.addDocument("</computera>");
    this.addDocument("</computerb>");
    this.addDocument("</computerc>");

    final NodeQuery q = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", false, false);

    ScoreDoc[] hits = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals("A,B,D, only B in range", 1, hits.length);

    this.addDocument("</computerc>");
    hits = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals("C added, still only B in range", 1, hits.length);
  }

  public void testInclusive1() throws Exception {
    this.addDocument("</computera>");
    this.addDocument("</computerb>");
    this.addDocument("</computerc>");
    this.addDocument("</computerd>");

    final NodeQuery q = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", true, true);

    final ScoreDoc[] hits = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals("A,B,C,D - A,B,C in range", 3, hits.length);
  }

  public void testInclusive2() throws Exception {
    this.addDocument("</computera>");
    this.addDocument("</computerb>");
    this.addDocument("</computerd>");

    final NodeQuery q = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", true, true);

    ScoreDoc[] hits = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals("A,B,D - A and B in range", 2, hits.length);

    this.addDocument("</computerc>");
    hits = searcher.search(dq(q), null, 1000).scoreDocs;
    assertEquals("C added - A, B, C in range", 3, hits.length);
  }

  public void testAllDocs() throws Exception {
    this.addDocuments(new String[]{"</computera>", "</computerb>", "</computerc>", "</computerd>"});

    NodeTermRangeQuery query = new NodeTermRangeQuery(DEFAULT_TEST_FIELD, null, null, true, true);
    final Terms terms = MultiFields.getTerms(searcher.getIndexReader(), DEFAULT_TEST_FIELD);
    assertFalse(query.getTermsEnum(terms) instanceof TermRangeTermsEnum);
    assertEquals(4, searcher.search(dq(query), null, 1000).scoreDocs.length);
    query = new NodeTermRangeQuery(DEFAULT_TEST_FIELD, null, null, false, false);
    assertFalse(query.getTermsEnum(terms) instanceof TermRangeTermsEnum);
    assertEquals(4, searcher.search(dq(query), null, 1000).scoreDocs.length);
    query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "", null, true, false);
    assertFalse(query.getTermsEnum(terms) instanceof TermRangeTermsEnum);
    assertEquals(4, searcher.search(dq(query), null, 1000).scoreDocs.length);
    // and now an other one
    query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computerb", null, true, false);
    assertTrue(query.getTermsEnum(terms) instanceof TermRangeTermsEnum);
    assertEquals(3, searcher.search(dq(query), null, 1000).scoreDocs.length);
    reader.close();
  }

  /** This test should not be here, but it tests the fuzzy query rewrite mode (TOP_TERMS_SCORING_BOOLEAN_REWRITE)
   * with constant score and checks, that only the lower end of terms is put into the range */
  public void testTopTermsRewrite() throws Exception {
    this.addDocuments(new String[]{"</computera>", "</computerb>", "</computerc>", "</computerd>", "</computere>", "</computerf>",
                                   "</computerg>", "</computerh>", "</computeri>", "</computerj>", "</computerk>"});

    final NodeTermRangeQuery query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computerb", "/computerj", true, true);
    this.checkBooleanTerms(query, "/computerb", "/computerc", "/computerd", "/computere", "/computerf",
      "/computerg", "/computerh", "/computeri", "/computerj");

    final int savedClauseCount = NodeBooleanQuery.getMaxClauseCount();
    try {
      NodeBooleanQuery.setMaxClauseCount(3);
      this.checkBooleanTerms(query, "/computerb", "/computerc", "/computerd");
    } finally {
      NodeBooleanQuery.setMaxClauseCount(savedClauseCount);
    }
  }

  private void checkBooleanTerms(final NodeTermRangeQuery query, final String... terms)
  throws IOException {
    query.setRewriteMethod(new MultiNodeTermQuery.TopTermsScoringNodeBooleanQueryRewrite(50));
    final NodeBooleanQuery bq = (NodeBooleanQuery) searcher.rewrite(query);
    final Set<String> allowedTerms = new HashSet<String>(Arrays.asList(terms));
    assertEquals(allowedTerms.size(), bq.clauses().size());
    for (final NodeBooleanClause c : bq.clauses()) {
      assertTrue(c.getQuery() instanceof NodeTermQuery);
      final NodeTermQuery tq = (NodeTermQuery) c.getQuery();
      final String term = tq.getTerm().text();
      assertTrue("invalid term: "+ term, allowedTerms.contains(term));
      allowedTerms.remove(term); // remove to fail on double terms
    }
    assertEquals(0, allowedTerms.size());
  }

  public void testEqualsHashcode() {
    Query query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", true, true);

    query.setBoost(1.0f);
    Query other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", true, true);
    other.setBoost(1.0f);

    assertEquals("query equals itself is true", query, query);
    assertEquals("equivalent queries are equal", query, other);
    assertEquals("hashcode must return same value when equals is true", query.hashCode(), other.hashCode());

    other.setBoost(2.0f);
    assertFalse("Different boost queries are not equal", query.equals(other));

    other = NodeTermRangeQuery.newStringRange("notcontent", "/computera", "/computerc", true, true);
    assertFalse("Different fields are not equal", query.equals(other));

    other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computerx", "/computerc", true, true);
    assertFalse("Different lower terms are not equal", query.equals(other));

    other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerz", true, true);
    assertFalse("Different upper terms are not equal", query.equals(other));

    query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, null, "/computerc", true, true);
    other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, null, "/computerc", true, true);
    assertEquals("equivalent queries with null lowerterms are equal()", query, other);
    assertEquals("hashcode must return same value when equals is true", query.hashCode(), other.hashCode());

    query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computerc", null, true, true);
    other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computerc", null, true, true);
    assertEquals("equivalent queries with null upperterms are equal()", query, other);
    assertEquals("hashcode returns same value", query.hashCode(), other.hashCode());

    query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, null, "/computerc", true, true);
    other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computerc", null, true, true);
    assertFalse("queries with different upper and lower terms are not equal", query.equals(other));

    query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", false, false);
    other = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "/computera", "/computerc", true, true);
    assertFalse("queries with different inclusive are not equal", query.equals(other));
  }

  /** Test the {@link com.sindicetech.siren.search.node.MultiNodeTermQuery.TopTermsBoostOnlyNodeBooleanQueryRewrite} rewrite method. */
  @Test
  public void testNodeConstraintsOnlyRewrite() throws Exception {
    this.addDocument("<aaa> <bbb> <ccc> <ddd>");
    this.addDocument("<bbb> <ddd> <aaa> <ccc>");
    this.addDocument("<ddd> <ccc> <aaa> <bbb>");

    final NodeTermRangeQuery query = NodeTermRangeQuery.newStringRange(DEFAULT_TEST_FIELD, "aaa", "bbb", true, true);
    query.setNodeConstraint(0, 1);
    query.setRewriteMethod(new MultiNodeTermQuery.TopTermsBoostOnlyNodeBooleanQueryRewrite(50));
    ScoreDoc[] hits = searcher.search(dq(query), null, 1000).scoreDocs;
    assertEquals(2, hits.length);

    query.setRewriteMethod(new MultiNodeTermQuery.TopTermsScoringNodeBooleanQueryRewrite(50));
    hits = searcher.search(dq(query), null, 1000).scoreDocs;
    assertEquals(2, hits.length);

    query.setRewriteMethod(new MultiNodeTermQuery.NodeConstantScoreAutoRewrite());
    hits = searcher.search(dq(query), null, 1000).scoreDocs;
    assertEquals(2, hits.length);

    query.setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    hits = searcher.search(dq(query), null, 1000).scoreDocs;
    assertEquals(2, hits.length);
  }

}