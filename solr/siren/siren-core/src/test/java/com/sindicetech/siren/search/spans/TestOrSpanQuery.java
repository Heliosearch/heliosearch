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
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestOrSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  @Test
  public void testSpanOrSingleDocument1() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(   doc( token("aaa", node(1)), token("bbb", node(1)))   );

    TermSpanQuery termA = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery termB = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery termC = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));
    TermSpanQuery termD = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ddd"));

    // ask for "aaa" or "bbb" - should return 2 hits 1 doc
    OrSpanQuery spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA, termB});
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testSpanOrSingleDocument() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(   doc( token("aaa", node(1)), token("bbb", node(1)))   );


    TermSpanQuery termA = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery termB = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery termC = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));
    TermSpanQuery termD = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ddd"));

    // ask for completely different 2 terms - should return 0
    OrSpanQuery spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termD});
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    // ask or query with just one span query
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    // ask or query with just one span query - should return 1
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);


    // ask for "aaa" or "bbb" - should return 2 hits 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA, termB});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);


    // ask for "aaa" or "ddd" - should return 1 hits 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA, termD});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);


    // ask for "bbb" or "ddd" - should return 1 hit 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termB, termD});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    // ask for "ccc" or "aaa" - should return 1 hit and 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termA});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    // ask for "ccc" or "bbb" - should return 1 hit and 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termB});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

  }

  @Test
  public void testSpanOrMultipleDocument4() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(
        doc(   token("aaa", node(1)), token("bbb", node(1,2)), token("ccc", node(1,2))  )
    );

    TermSpanQuery termA = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery termB = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery termC = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));

    // ask for completely different 2 terms - should return 0
    OrSpanQuery spanOr = new OrSpanQuery(new TermSpanQuery[] {termA, termB});
    NearSpanQuery near = new NearSpanQuery(new SpanQuery[] { spanOr, termC },  0,  true);
    Query query = new LuceneProxyNodeQuery(near);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testSpanOrMultipleDocument() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(
        doc(   token("aaa", node(1))  ),
        doc(   token("bbb", node(1))  )
    );

    TermSpanQuery termA = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery termB = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery termC = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));
    TermSpanQuery termD = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ddd"));


    // ask for completely different 2 terms - should return 0
    OrSpanQuery spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termD});
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
    assertEquals(0, hits.scoreDocs.length);


    // ask or query with just one span query
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
    assertEquals(0, hits.scoreDocs.length);


    // ask or query with just one span query - should return 1 hit 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);


    // ask for "aaa" or "bbb" - should return 2 hits 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA, termB});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(2, hits.totalHits);
    assertEquals(2, hits.scoreDocs.length);

    // ask for "aaa" or "ddd" - should return 1 hits 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA, termD});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);


    // ask for "bbb" or "ddd" - should return 1 hit 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termB, termD});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);


    // ask for "ccc" or "aaa" - should return 1 hit and 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termA});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);


    // ask for "ccc" or "bbb" - should return 1 hit and 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termB});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);
  }


  @Test
  public void testSpanOrMultipleDocument2() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(
        doc(   token("aaa", node(1)), token("bbb", node(1))   ),
        doc(   token("bbb", node(1))  ),
        doc(   token("ccc", node(1))  ),
        doc(   token("ddd", node(1)), token("bbb", node(1))   )
    );

    TermSpanQuery termA = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery termB = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery termC = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));
    TermSpanQuery termD = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ddd"));


    // ask for completely different 2 terms - should return 2 hits 2 documents
    OrSpanQuery spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC, termD});
    Query query = new LuceneProxyNodeQuery(spanQuery);

    TopDocs hits = searcher.search(query, 100);
    assertEquals(2, hits.totalHits);
    assertEquals(2, hits.scoreDocs.length);


    // ask or query with just one span query
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termC});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);


    // ask or query with just one span query - should return 1 hit 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termA});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
    assertEquals(1, hits.scoreDocs.length);

    // ask or query with just one span query - should return 1 hit 1 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termB});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(3, hits.totalHits);
    assertEquals(3, hits.scoreDocs.length);


    // ask or query - should return 4 hit 4 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termB,termC});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(4, hits.totalHits);
    assertEquals(4, hits.scoreDocs.length);

    // ask or query  - should return 4 hit 3 doc
    spanQuery = new OrSpanQuery(new TermSpanQuery[] {termB,termD});
    query = new LuceneProxyNodeQuery(spanQuery);

    hits = searcher.search(query, 100);
    assertEquals(3, hits.totalHits);
  }

  @Test
  public void testSpanOrNested() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(
        doc(   token("aaa", node(1))  ),
        doc(   token("bbb", node(1))  ),
        doc(   token("ccc", node(1))  ),
        doc(   token("ddd", node(1))  )
    );

    TermSpanQuery termA = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery termB = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery termC = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));
    TermSpanQuery termD = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ddd"));

    OrSpanQuery aOrB = new OrSpanQuery(new TermSpanQuery[] {termA,termB});
    OrSpanQuery bOrC = new OrSpanQuery(new TermSpanQuery[] {termB,termC});

    OrSpanQuery spanQuery = new OrSpanQuery(new SpanQuery[] {aOrB,bOrC});

    Query query = new LuceneProxyNodeQuery(spanQuery);
    TopDocs hits = searcher.search(query, 100);
    assertEquals(3, hits.totalHits);
  }

  @Test
  public void testEquality() throws Exception {
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    OrSpanQuery or1 = new OrSpanQuery(new SpanQuery[] {term1, term2});

    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    OrSpanQuery or2 = new OrSpanQuery(new SpanQuery[] {term3, term4});

    assertEquals(or1, or2);

    OrSpanQuery or3 = new OrSpanQuery(new SpanQuery[] {term1, term2});
    or3.setLevelConstraint(3);
    assertNotEquals(or1, or3);

    OrSpanQuery or4 = new OrSpanQuery(new SpanQuery[] {term1, term2});
    or4.setNodeConstraint(5);
    assertNotEquals(or1, or4);
  }

  @Test
  public void testSetLevelConstraint() {
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    OrSpanQuery or1 = new OrSpanQuery(new SpanQuery[] {term1, term2});

    or1.setLevelConstraint(3);
    assertEquals(3, or1.getLevelConstraint());
    // Level constraint must have been transferred to the clauses
    assertEquals(3, or1.getClauses()[0].getLevelConstraint());
    assertEquals(3, or1.getClauses()[1].getLevelConstraint());

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("bbb").getQuery());
    OrSpanQuery or2 = new OrSpanQuery(new SpanQuery[] {term3, term4});
    or2.setLevelConstraint(4);

    OrSpanQuery or3 = new OrSpanQuery(new SpanQuery[] {or1, or2});
    or3.setLevelConstraint(6);

    // Level constraint must have been transferred to the clauses
    assertEquals(6, or1.getLevelConstraint());
    assertEquals(6, or2.getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    OrSpanQuery or1 = new OrSpanQuery(new SpanQuery[] {term1, term2});

    final TwigQuery twig = new TwigQuery();

    or1.setAncestorPointer(twig);

    assertSame(twig, or1.getAncestorPointer());
    // clauses must have been updated
    assertSame(twig, term1.getAncestorPointer());

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    or1.addClause(term3);
    // new clause must have been updated
    assertSame(twig, term3.getAncestorPointer());
  }

}
