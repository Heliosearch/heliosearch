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

import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.junit.Test;

import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;

public class TestLuceneProxyNodeQuery
extends AbstractTestSirenScorer {

  @Override
  protected void configure()
  throws IOException {
    this.setAnalyzer(AnalyzerType.TUPLE);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Test
  public void testBoost()
  throws Exception {
    final float boost = 2.5f;

    this.addDocument("\"aaa ccc\" \"one five\" . \"aaa bbb\" \"ccc eee\" .");

    BooleanQuery bq1 = new BooleanQuery();
    NodeTermQuery tq = new NodeTermQuery(new Term (DEFAULT_TEST_FIELD, "one"));
    tq.setBoost(boost);
    bq1.add(new LuceneProxyNodeQuery(tq), Occur.MUST);
    bq1.add(new LuceneProxyNodeQuery(new NodeTermQuery(new Term (DEFAULT_TEST_FIELD, "five"))), Occur.MUST);

    BooleanQuery bq2 = new BooleanQuery();
    tq = new NodeTermQuery(new Term (DEFAULT_TEST_FIELD, "one"));
    LuceneProxyNodeQuery dq = new LuceneProxyNodeQuery(tq);
    dq.setBoost(boost);
    bq2.add(dq, Occur.MUST);
    bq2.add(new LuceneProxyNodeQuery(new NodeTermQuery(new Term (DEFAULT_TEST_FIELD, "five"))), Occur.MUST);

    assertScoreEquals(bq1, bq2);
  }

  /**
   * Tests whether the scores of the two queries are the same.
   */
  public void assertScoreEquals(Query q1, Query q2)
  throws Exception {
    ScoreDoc[] hits1 = searcher.search (q1, null, 1000).scoreDocs;
    ScoreDoc[] hits2 = searcher.search (q2, null, 1000).scoreDocs;

    assertEquals(hits1.length, hits2.length);

    for (int i = 0; i < hits1.length; i++) {
      assertEquals(hits1[i].score, hits2[i].score, 0.0000001f);
    }
  }

  @Test
  public void testExplain()
  throws Exception {
    this.setAnalyzer(AnalyzerType.JSON);

    this.addDocument("{\"aaa\" : \"bbb\"}");

    Query query = ntq("aaa").getLuceneProxyQuery();
    final Explanation exp = searcher.explain(query, 0);
    assertTrue(exp.getValue() != 0);
  }

}
