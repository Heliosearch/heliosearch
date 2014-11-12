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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeTermQuery;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.search.spans.BooleanSpanQuery;
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.search.spans.TermSpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestBooleanSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  @Test
  public void testAsTwigChild() throws Exception {
    this.addDocuments(
      doc(token("aa", node(1)), token("aaa", node(1,1)), token("bbb", node(1,3))),
      doc(token("bb", node(1)), token("aaa", node(1,1)), token("bbb", node(1,3)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());

    BooleanSpanQuery bsq = new BooleanSpanQuery(1, true);
    bsq.add(term1, NodeBooleanClause.Occur.MUST);
    bsq.add(term2, NodeBooleanClause.Occur.MUST);

    TwigQuery twig = new TwigQuery();
    twig.addRoot(ntq("aa").getQuery());
    twig.addChild(bsq, NodeBooleanClause.Occur.MUST);

    Query query = new LuceneProxyNodeQuery(twig);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testAsTwigDescendant() throws Exception {
    this.addDocuments(
      doc(token("aa", node(1)), token("aaa", node(1,1)), token("bbb", node(1,3))),
      doc(token("aa", node(1)), token("aaa", node(1,1,1)), token("bbb", node(1,1,3))),
      doc(token("bb", node(1)), token("aaa", node(1,1,1)), token("bbb", node(1,1,3)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());

    BooleanSpanQuery bsq = new BooleanSpanQuery(1, true);
    bsq.add(term1, NodeBooleanClause.Occur.MUST);
    bsq.add(term2, NodeBooleanClause.Occur.MUST);

    TwigQuery twig = new TwigQuery();
    twig.addRoot(ntq("aa").getQuery());
    twig.addDescendant(2, bsq, NodeBooleanClause.Occur.MUST);

    Query query = new LuceneProxyNodeQuery(twig);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNotSameNode() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(2)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));

    final BooleanSpanQuery bsq1 = new BooleanSpanQuery(0, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);

    Query query = new LuceneProxyNodeQuery(bsq1);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  @Test
  public void testReqWithSlop() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(1), 2))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));

    // should not match
    BooleanSpanQuery bsq1 = new BooleanSpanQuery(0, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);
    Query query = new LuceneProxyNodeQuery(bsq1);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    // should match
    bsq1 = new BooleanSpanQuery(1, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);
    query = new LuceneProxyNodeQuery(bsq1);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    bsq1 = new BooleanSpanQuery(2, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);
    query = new LuceneProxyNodeQuery(bsq1);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testReqOptWithSlop() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("ccc", node(1), 2), token("bbb", node(1))),
      doc(token("aaa", node(1)), token("ccc", node(1)), token("bbb", node(1))),
      doc(token("aaa", node(1)), token("bbb", node(1)), token("ccc", node(1))),
      doc(token("aaa", node(1)), token("ccc", node(1), 2), token("bbb", node(1), 2))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));

    BooleanSpanQuery bsq1 = new BooleanSpanQuery(1, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.SHOULD);
    bsq1.add(term3, NodeBooleanClause.Occur.MUST);
    Query query = new LuceneProxyNodeQuery(bsq1);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(4, hits.totalHits);

    float[] scores = new float[4];
    for (int i = 0; i < 4; i ++) {
      scores[hits.scoreDocs[i].doc] = hits.scoreDocs[i].score;
    }
    // second and third documents match without slop and matches the optional term bbb
    assertEquals(scores[1], scores[2], 0);
    // first document matches with a slop of 1 and matches the optional term bbb
    assertTrue(scores[0] < scores[1]);
    // fourth document matches with a slop of 1 but does not match the optional term bbb
    assertTrue(scores[3] < scores[0]);
  }

  @Test
  public void testReqExcludeWithSlop() throws IOException {
    this.addDocuments(
      doc(token("aaa", node(1,2)), token("bbb", node(1,2)), token("ccc", node(1,2))),
      doc(token("aaa", node(1,3)), token("ccc", node(1,3)), token("bbb", node(1,3)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));

    BooleanSpanQuery bsq1 = new BooleanSpanQuery(1, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST_NOT);
    bsq1.add(term3, NodeBooleanClause.Occur.MUST);
    Query query = new LuceneProxyNodeQuery(bsq1);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testEquality() throws Exception {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    final BooleanSpanQuery bsq1 = new BooleanSpanQuery(0, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("bbb").getQuery());
    final BooleanSpanQuery bsq2 = new BooleanSpanQuery(0, true);
    bsq2.add(term3, NodeBooleanClause.Occur.MUST);
    bsq2.add(term4, NodeBooleanClause.Occur.MUST);

    assertEquals(bsq1, bsq2);

    final BooleanSpanQuery bsq3 = new BooleanSpanQuery(1, true);
    bsq3.add(term3, NodeBooleanClause.Occur.MUST);
    bsq3.add(term4, NodeBooleanClause.Occur.MUST);

    assertNotEquals(bsq1, bsq3);

    NodeSpanQuery term5 = new NodeSpanQuery(ntq("ccc").getQuery());
    final BooleanSpanQuery bsq4 = new BooleanSpanQuery(0, true);
    bsq4.add(term1, NodeBooleanClause.Occur.MUST);
    bsq4.add(term5, NodeBooleanClause.Occur.MUST);

    assertNotEquals(bsq1, bsq4);

    final BooleanSpanQuery bsq5 = new BooleanSpanQuery(0, true);
    bsq5.add(term1, NodeBooleanClause.Occur.MUST);
    bsq5.add(term2, NodeBooleanClause.Occur.MUST);
    bsq5.setLevelConstraint(3);

    assertNotEquals(bsq1, bsq5);

    final BooleanSpanQuery bsq6 = new BooleanSpanQuery(0, true);
    bsq6.add(term1, NodeBooleanClause.Occur.MUST);
    bsq6.add(term2, NodeBooleanClause.Occur.MUST);
    bsq6.setNodeConstraint(5);

    assertNotEquals(bsq1, bsq6);
  }

  @Test
  public void testSetLevelConstraint() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    final BooleanSpanQuery bsq1 = new BooleanSpanQuery(0, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);

    bsq1.setLevelConstraint(3);
    assertEquals(3, bsq1.getLevelConstraint());
    // Level constraint must have been transferred to the clauses
    assertEquals(3, bsq1.getClauses()[0].getQuery().getLevelConstraint());
    assertEquals(3, bsq1.getClauses()[1].getQuery().getLevelConstraint());

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("bbb").getQuery());
    final BooleanSpanQuery bsq2 = new BooleanSpanQuery(0, true);
    bsq2.add(term3, NodeBooleanClause.Occur.MUST);
    bsq2.add(term4, NodeBooleanClause.Occur.MUST);
    bsq2.setLevelConstraint(4);

    final NearSpanQuery nsq3 = new NearSpanQuery(new SpanQuery[] {bsq1, bsq2}, 0, true);
    nsq3.setLevelConstraint(6);

    // Level constraint must have been transferred to the clauses
    assertEquals(6, bsq1.getLevelConstraint());
    assertEquals(6, bsq1.getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    final BooleanSpanQuery bsq1 = new BooleanSpanQuery(0, true);
    bsq1.add(term1, NodeBooleanClause.Occur.MUST);
    bsq1.add(term2, NodeBooleanClause.Occur.MUST);

    final TwigQuery twig = new TwigQuery();

    bsq1.setAncestorPointer(twig);

    assertSame(twig, bsq1.getAncestorPointer());
    // clauses must have been updated
    assertSame(twig, term1.getAncestorPointer());
  }

  @Test
  public void testLevelAndNodeConstraintPropagation() throws IOException {
    final TwigQuery tq1 = new TwigQuery(2);
    tq1.addDescendant(2, new NodeTermQuery(new Term("field", "aaa")), NodeBooleanClause.Occur.MUST);
    tq1.setNodeConstraint(1,2);

    final TwigQuery tq2 = new TwigQuery(2);
    tq2.addChild(new NodeTermQuery(new Term("field", "bbb")), NodeBooleanClause.Occur.MUST);

    final BooleanSpanQuery bsq1 = new BooleanSpanQuery(0, true);
    bsq1.add(new NodeSpanQuery(tq1), NodeBooleanClause.Occur.MUST);
    bsq1.add(new NodeSpanQuery(tq2), NodeBooleanClause.Occur.MUST);

    // Level constraints applied on the twig must not be modified

    assertEquals(2, tq1.getLevelConstraint());
    assertEquals(4, tq1.clauses().get(0).getQuery().getLevelConstraint());

    assertEquals(2, tq2.getLevelConstraint());
    assertEquals(3, tq2.clauses().get(0).getQuery().getLevelConstraint());

    // Node constraints applied on the twig must not be modified

    assertEquals(1, tq1.getNodeConstraint()[0]);
    assertEquals(2, tq1.getNodeConstraint()[1]);

    // Constraints should not be modified after a rewrite

    final BooleanSpanQuery rewritten = (BooleanSpanQuery) bsq1.rewrite(reader);
    NodeSpanQuery nodeSpanQuery = (NodeSpanQuery) rewritten.getClauses()[0].getQuery();
    assertEquals(2, nodeSpanQuery.getQuery().getLevelConstraint());
    assertEquals(1, nodeSpanQuery.getQuery().getNodeConstraint()[0]);
    assertEquals(2, nodeSpanQuery.getQuery().getNodeConstraint()[1]);

    nodeSpanQuery = (NodeSpanQuery) rewritten.getClauses()[1].getQuery();
    assertEquals(2, nodeSpanQuery.getQuery().getLevelConstraint());
  }

}
