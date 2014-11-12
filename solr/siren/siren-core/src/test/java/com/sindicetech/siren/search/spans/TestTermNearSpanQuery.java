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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.search.spans.TermSpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;

public class TestTermNearSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  @Test
  public void testNotSameNode() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(2)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    NearSpanQuery spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  @Test
  public void testNearOrdered() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(1))),
      doc(token("bbb", node(1)), token("aaa", node(1)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    NearSpanQuery spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 1, true);
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNearOrderedSlop() throws Exception {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(1), 2))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));

    // should not match
    NearSpanQuery spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    // should match
    spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 1, true);
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 2, true);
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNearOrderedNested() throws Exception {
    this.addDocuments(
      doc(token("three", node(1)), token("hundred", node(1)), token("thirty", node(1)), token("three", node(1))),
      doc(token("three", node(1)), token("hundred", node(1)), token("thirty", node(1)), token("four", node(1)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "hundred"));
    NearSpanQuery near1 = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "thirty"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    NearSpanQuery near2 = new NearSpanQuery(new TermSpanQuery[] {term3, term4}, 0, true);

    NearSpanQuery spanQuery = new NearSpanQuery(new SpanQuery[] {near1, near2}, 0, true);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNearUnorderedNested() throws Exception {
    this.addDocuments(
      doc(token("three", node(1)), token("hundred", node(1)), token("thirty", node(1)), token("three", node(1))),
      doc(token("three", node(1)), token("hundred", node(1)), token("thirty", node(1)), token("four", node(1))),
      doc(token("three", node(1)), token("hundred", node(1)), token("thirty", node(1), 2), token("three", node(1)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "thirty"));
    NearSpanQuery near1 = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, false);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "hundred"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    NearSpanQuery near2 = new NearSpanQuery(new TermSpanQuery[] {term3, term4}, 0, false);

    NearSpanQuery spanQuery = new NearSpanQuery(new SpanQuery[] {near1, near2}, 0, false);
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testNearExactOrderedAfterInvalidSpan() throws IOException {
    this.addDocuments(doc(token("aaa", node(2)), token("bbb", node(2), 2), token("aaa", node(2)), token("bbb", node(2))));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    Query query = new LuceneProxyNodeQuery(new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 0, true));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testExplain() throws IOException {
    this.addDocuments(
      doc(token("three", node(1)), token("hundred", node(1)), token("thirty", node(1)), token("three", node(1)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "thirty"));
    NearSpanQuery near1 = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, false);

    Query query = new LuceneProxyNodeQuery(near1);

    Explanation explanation = searcher.explain(query, 0);
    assertNotNull(explanation);
  }

}
