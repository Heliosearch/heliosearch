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

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestNodeNearSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  @Test
  public void testNotSameParent() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1,1)), token("bbb", node(2,2)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    NearSpanQuery spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  @Test
  public void testNearOrdered() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1,1)), token("bbb", node(1,2))),
      doc(token("bbb", node(1,1)), token("aaa", node(1,2)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    NearSpanQuery spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 1, true);
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  /**
   * Case with nodes from level 1. The parent is the document root which is represented by an empty
   * {@link org.apache.lucene.util.IntsRef}
   */
  @Test
  public void testNearOrderedFirstLevel() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(2))),
      doc(token("bbb", node(1)), token("aaa", node(2)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    NearSpanQuery spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNearOrderedSlop() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1,1)), token("bbb", node(1,3)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());

    // should not match
    NearSpanQuery spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    // should match
    spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 1, true);
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 2, true);
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNearOrderedNested() throws Exception {
    this.addDocuments(
      doc(token("three", node(1,1)), token("hundred", node(1,2)), token("thirty", node(1,3)), token("three", node(1,4))),
      doc(token("three", node(1,1)), token("hundred", node(1,2)), token("thirty", node(1,3)), token("four", node(1,4)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("three").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("hundred").getQuery());
    NearSpanQuery near1 = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    NodeSpanQuery term3 = new NodeSpanQuery(ntq("thirty").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("three").getQuery());
    NearSpanQuery near2 = new NearSpanQuery(new NodeSpanQuery[] {term3, term4}, 0, true);

    NearSpanQuery spanQuery = new NearSpanQuery(new SpanQuery[] {near1, near2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testTwigWithNear() throws Exception {
    this.addDocuments(
      doc(token("aa", node(1)), token("aaa", node(1,1)), token("bbb", node(1,3))),
      doc(token("bb", node(1)), token("aaa", node(1,1)), token("bbb", node(1,3)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    NearSpanQuery spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 1, true);

    TwigQuery twig = new TwigQuery();
    twig.addRoot(ntq("aa").getQuery());
    twig.addChild(spanQuery, NodeBooleanClause.Occur.MUST);

    Query query = new LuceneProxyNodeQuery(twig);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testExplain() throws IOException {
    this.addDocuments(
      doc(token("three", node(1,1)), token("hundred", node(1,2)), token("thirty", node(1,3)), token("three", node(1,4)))
    );

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("three").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("thirty").getQuery());
    NearSpanQuery near1 = new NearSpanQuery(new SpanQuery[] {term1, term2}, 0, false);

    Query query = new LuceneProxyNodeQuery(near1);

    Explanation explanation = searcher.explain(query, 0);
    assertNotNull(explanation);
  }

}
