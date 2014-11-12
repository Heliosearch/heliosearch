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

public class TestPositionRangeSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  @Test
  public void testTermSpanPositionRange() throws Exception {
    // first test with TermSpanQuery on a single node
    this.addDocuments(   doc( token("11", node(1))   ),
                         doc( token("21", node(1))   ),

                         doc( token("11", node(1)    ), token("12", node(1) )    ),
                         doc( token("21", node(1)    ), token("22", node(1) )    ),

                         doc( token("11", node(1)    ), token("12", node(1) ), token("13", node(1)   )    ),
                         doc( token("21", node(1)    ), token("22", node(1) ), token("23", node(1)   )    )
                      );

    TermSpanQuery termOne = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "11"));
    TermSpanQuery termTwo = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "12"));
    TermSpanQuery termTree= new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "13"));


    SpanQuery termSpanRange = new PositionRangeSpanQuery(termOne, 0, 1);
    Query query = new LuceneProxyNodeQuery(termSpanRange);
    TopDocs hits = searcher.search(query, 100);
    assertEquals(3, hits.totalHits);

    termSpanRange = new PositionRangeSpanQuery(termOne, 1, 3);
    query = new LuceneProxyNodeQuery(termSpanRange);
    hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    //======================

    termSpanRange = new PositionRangeSpanQuery(termTwo, 1, 2);
    query = new LuceneProxyNodeQuery(termSpanRange);
    hits = searcher.search(query, 100);
    assertEquals(2, hits.totalHits);

    termSpanRange = new PositionRangeSpanQuery(termTwo, 2, 3);
    query = new LuceneProxyNodeQuery(termSpanRange);
    hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);

    //======================

    termSpanRange = new PositionRangeSpanQuery(termTree, 2, 3);
    query = new LuceneProxyNodeQuery(termSpanRange);
    hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);

    termSpanRange = new PositionRangeSpanQuery(termTree, 3, 4);
    query = new LuceneProxyNodeQuery(termSpanRange);
    hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  @Test
  public void testEquality() throws Exception {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    final PositionRangeSpanQuery prsq1 = new PositionRangeSpanQuery(term1, 3, 4);

    NodeSpanQuery term2 = new NodeSpanQuery(ntq("aaa").getQuery());
    final PositionRangeSpanQuery prsq2 = new PositionRangeSpanQuery(term2, 3, 4);

    assertEquals(prsq1, prsq2);

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("bbb").getQuery());
    final PositionRangeSpanQuery prsq3 = new PositionRangeSpanQuery(term3, 3, 4);

    assertNotEquals(prsq1, prsq3);

    final PositionRangeSpanQuery prsq4 = new PositionRangeSpanQuery(term1, 0, 2);

    assertNotEquals(prsq1, prsq4);

    final PositionRangeSpanQuery prsq5 = new PositionRangeSpanQuery(term1, 3, 4);
    prsq5.setLevelConstraint(3);
    assertNotEquals(prsq1, prsq5);

    final PositionRangeSpanQuery prsq6 = new PositionRangeSpanQuery(term1, 3, 4);
    prsq6.setNodeConstraint(5);
    assertNotEquals(prsq1, prsq6);
  }

  @Test
  public void testSetLevelConstraint() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    final PositionRangeSpanQuery prsq1 = new PositionRangeSpanQuery(term1, 3, 4);

    prsq1.setLevelConstraint(3);
    assertEquals(3, prsq1.getLevelConstraint());
    // Level constraint must have been transferred to the clauses
    assertEquals(3, prsq1.getMatch().getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    final PositionRangeSpanQuery prsq1 = new PositionRangeSpanQuery(term1, 3, 4);

    final TwigQuery twig = new TwigQuery();

    prsq1.setAncestorPointer(twig);

    assertSame(twig, prsq1.getAncestorPointer());
    // clauses must have been updated
    assertSame(twig, term1.getAncestorPointer());
  }

}
