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

import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.must;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeBooleanQueryBuilder.nbq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.TupleQueryBuilder.tuple;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.util.BasicSirenTestCase;
import com.sindicetech.siren.util.XSDDatatype;

public class TestBooleanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.TUPLE);
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    ((TupleAnalyzer) analyzer).registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Test
  public void testReqTuple() throws CorruptIndexException, IOException {
    for (int i = 0; i < 10; i++) {
      this.addDocument("<subj> <aaa> <bbb> . <subj> <ccc> <ddd> . ");
      this.addDocument("<subj> <aaa> <bbb> . ");
    }

    final Query nested1 = tuple().with(nbq(must("aaa")).bound(1,1))
                                 .with(nbq(must("bbb")).bound(2,2))
                                 .getLuceneProxyQuery();

    final Query nested2 = tuple().with(nbq(must("ccc")).bound(1,1))
                                 .with(nbq(must("ddd")).bound(2,2))
                                 .getLuceneProxyQuery();

    final BooleanQuery q = new BooleanQuery();
    q.add(nested1, Occur.MUST);
    q.add(nested2, Occur.MUST);

    assertEquals(10, searcher.search(q, 10).totalHits);
  }

  @Test
  public void testReqOptTuple() throws CorruptIndexException, IOException {
    for (int i = 0; i < 10; i++) {
      this.addDocument("<subj> <aaa> <bbb> . <subj> <ccc> <ddd> . ");
      this.addDocument("<subj> <aaa> <bbb> . ");
    }

    final Query nested1 = tuple().with(nbq(must("aaa")).bound(1,1))
                                 .with(nbq(must("bbb")).bound(2,2))
                                 .getLuceneProxyQuery();

    final Query nested2 = tuple().with(nbq(must("ccc")).bound(1,1))
                                 .with(nbq(must("ddd")).bound(2,2))
                                 .getLuceneProxyQuery();

    final BooleanQuery q = new BooleanQuery();
    q.add(nested1, Occur.MUST);
    q.add(nested2, Occur.SHOULD);

    assertEquals(20, searcher.search(q, 10).totalHits);
  }

  @Test
  public void testReqExclTuple() throws CorruptIndexException, IOException {
    for (int i = 0; i < 10; i++) {
      this.addDocument("<subj> <aaa> <bbb> . <subj> <ccc> <ddd> . <subj> <eee> <fff> . ");
      this.addDocument("<subj> <aaa> <bbb> . <subj> <ccc> <ddd> . <subj> <eee> <ggg> . ");
    }

    final Query nested1 = tuple().with(nbq(must("eee")).bound(1,1))
                                 .with(nbq(must("ggg")).bound(2,2))
                                 .getLuceneProxyQuery();

    final Query nested2 = tuple().with(nbq(must("aaa")).bound(1,1))
                                 .with(nbq(must("bbb")).bound(2,2))
                                 .getLuceneProxyQuery();

    final Query nested3 = tuple().with(nbq(must("ccc")).bound(1,1))
                                 .with(nbq(must("ddd")).bound(2,2))
                                 .getLuceneProxyQuery();

    final BooleanQuery q = new BooleanQuery();
    q.add(nested1, Occur.MUST_NOT);
    q.add(nested2, Occur.MUST);
    q.add(nested3, Occur.MUST);

    assertEquals(10, searcher.search(q, 10).totalHits);
  }

  @Test
  public void testReqExclCell() throws CorruptIndexException, IOException {
    for (int i = 0; i < 10; i++) {
      this.addDocument("<subj> <aaa> <bbb> . <subj> <ccc> <ddd> . <subj> <eee> <fff> . ");
      this.addDocument("<subj> <aaa> <bbb> . <subj> <ccc> <ddd> . <subj> <eee> <ggg> . ");
    }

    final Query nested1 = nbq(must("aaa")).bound(1,1).getLuceneProxyQuery();
    final Query nested2 = nbq(must("bbb")).bound(2,2).getLuceneProxyQuery();
    final Query nested3 = nbq(must("ggg")).bound(2,2).getLuceneProxyQuery();

    final BooleanQuery q = new BooleanQuery();
    q.add(nested3, Occur.MUST_NOT);
    q.add(nested1, Occur.MUST);
    q.add(nested2, Occur.MUST);

    assertEquals(10, searcher.search(q, 10).totalHits);
  }

}
