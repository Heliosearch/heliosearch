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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.junit.Test;

import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.AbstractTestSirenScorer;

import java.io.IOException;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.must;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeBooleanQueryBuilder.nbq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestLuceneProxyNodeScorer extends AbstractTestSirenScorer {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.JSON);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Test
  public void testNextDoc()
  throws Exception {
    this.addDocuments(
      "{ \"aaa bbb\" : \"aaa ccc\" , \"ccc\" : \"bbb ccc\" }",
      "{ \"aaa\" : \"aaa bbb ddd\" }"
    );

    final Scorer scorer1 = this.getScorer(
      ntq("aaa").getLuceneProxyQuery()
    );

    assertTrue(scorer1.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, scorer1.docID());
    assertEquals(2, scorer1.freq(), 0);
    assertTrue(scorer1.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, scorer1.docID());
    assertEquals(2, scorer1.freq(), 0);
    assertTrue(scorer1.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);

    final Scorer scorer2 = this.getScorer(
      ntq("ccc").getLuceneProxyQuery()
    );

    assertTrue(scorer2.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(0, scorer2.docID());
    assertEquals(3, scorer2.freq(), 0);
    assertTrue(scorer2.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);

    final Scorer scorer3 = this.getScorer(
      ntq("ddd").getLuceneProxyQuery()
    );

    assertTrue(scorer3.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, scorer3.docID());
    assertEquals(1, scorer3.freq(), 0);
    assertTrue(scorer3.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
  }

  @Test
  public void testAdvance()
  throws Exception {
    this.addDocuments(
      "{ \"baba\" : \"aaa ccc\" , \"ccc\" : \"bbb ccc\" }",
      "{ \"aaa\" : \"aaa bbb ddd\" }",
      "{ \"ddd\" : [ \"bobo\", \"bibi\" ] }"
    );

    final Scorer scorer1 = this.getScorer(
      ntq("bobo").getLuceneProxyQuery()
    );

    assertTrue(scorer1.advance(2) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(2, scorer1.docID());
    assertEquals(1, scorer1.freq(), 0);
    assertTrue(scorer1.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);

    final Scorer scorer2 = this.getScorer(
      ntq("baba").getLuceneProxyQuery()
    );
    assertTrue(scorer2.advance(2) == DocIdSetIterator.NO_MORE_DOCS);
  }

  @Test
  public void testAdvanceInfiniteLoop()
  throws Exception {
    this.addDocuments(
      "{ \"baba\" : \"bibi ccc\" , \"ccc\" : \"bbb ccc\" }",
      "{ \"baba bibi baba bibi\" : \"aaa bbb ddd\" }",
      "{ \"baba bibi\" : \"aaa bbb ddd\" }"
    );

    final Scorer scorer1 = this.getScorer(
      nbq(must("baba", "bibi")).getLuceneProxyQuery()
    );

    assertTrue(scorer1.advance(0) != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, scorer1.docID());
    assertEquals(2, scorer1.freq(), 0);
    final float score1 = scorer1.score();
    assertTrue(scorer1.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(2, scorer1.docID());
    assertEquals(2, scorer1.freq(), 0);
    final float score2 = scorer1.score();
    assertTrue(score1 > score2);
    assertTrue(scorer1.nextDoc() == DocIdSetIterator.NO_MORE_DOCS);
  }

}
