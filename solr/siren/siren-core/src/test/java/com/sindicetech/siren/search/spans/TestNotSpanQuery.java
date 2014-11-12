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
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.NotSpanQuery;
import com.sindicetech.siren.search.spans.TermSpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;

public class TestNotSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  private TermSpanQuery fish = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "fish"));
  private TermSpanQuery chips = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "chips"));

  @Test
  public void testNonExistingExcludeTerm() throws IOException {
    this.addDocuments(
      doc(token("aaa", node(1)), token("bbb", node(1)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));

    NearSpanQuery span = new NearSpanQuery(new TermSpanQuery[]{ term1, term2 }, 0, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(span, term3));

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  @Test
  public void testExcludeNotSameNode() throws IOException {
    this.addDocuments(
            doc(token("aaa", node(1)), token("bbb", node(1)),
                token("aaa", node(1,2)), token("bbb", node(1,2)), token("ccc", node(1,2)),
                token("aaa", node(1,3)), token("ccc", node(1,3)))
    );

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "ccc"));

    NearSpanQuery span = new NearSpanQuery(new TermSpanQuery[]{ term1, term3 }, 1, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(span, term2));

    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  // exclude query match part, should be also filtered
  @Test
  public void testNotQueryMatchesNearInclude() throws IOException {
    this.addDocuments(doc(token("fish", node(1)), token("chips", node(1))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 0, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(fishandchips, fish));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  // exclude query match contains all include query match, there should be no result
  @Test
  public void testNotQueryMatchesNearExclude() throws IOException {
    this.addDocuments(doc(token("fish", node(1)), token("and", node(1)), token("chips", node(1)),
        token("somethink", node(1))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 1,
        true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(fish, fishandchips));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  // exclude query match contains all include query match but there is another match that is not in
  // exclude, there should be one result
  @Test
  public void testNotQueryMatchesButThereIsAnotherPosition() throws IOException {
    this.addDocuments(doc(token("somethink", node(1)), token("fish", node(1)),
        token("chips", node(1)), token("rise", node(1)), token("and", node(1)),
        token("fish", node(1))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 0,
        true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(fish, fishandchips));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  // exclude query match contains all include query matches in second node, there should be no
  // result
  @Test
  public void testNotQueryMatchesNearExcludeTwoTimes() throws IOException {
    this.addDocuments(doc(token("flash", node(1)), token("chips", node(2)), token("fish", node(2)),
        token("and", node(2)), token("chips", node(2)), token("somethink", node(2)),
        token("and", node(2)), token("fish", node(2)), token("chips", node(2))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 1,
        false);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(chips, fishandchips));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  // tests exclude within pre and post range
  // not in range should not filter
  @Test
  public void testNotQueryMatchesRangeNotFilter() throws IOException {
    this.addDocuments(doc(token("fish", node(2)), token("and", node(2)), token("chips", node(2)),
        token("is", node(2)), token("not", node(2)), token("good", node(2)), token("fish", node(2))));
    this.addDocuments(doc(token("good", node(2)), token("fish", node(2)), token("is", node(2)),
        token("not", node(2)), token("fish", node(2)), token("and", node(2)),
        token("chips", node(2))));

    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 1, false);
    NearSpanQuery goodFish = new NearSpanQuery(new TermSpanQuery[]{
        new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "good")), fish}, 0, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(goodFish, fishandchips, 2));

    TopDocs hits = searcher.search(query, 100);
    assertEquals(2, hits.totalHits);
  }

  // exclude in range should filter
  @Test
  public void testNotQueryMatchesRange() throws IOException {
    this.addDocuments(doc(token("fish", node(2)), token("and", node(2)), token("chips", node(2)),
        token("is", node(2)), token("not", node(2)), token("good", node(2)), token("fish", node(2))));
    this.addDocuments(doc(token("good", node(2)), token("fish", node(2)), token("is", node(2)),
        token("not", node(2)), token("fish", node(2)), token("and", node(2)),
        token("chips", node(2))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 1, false);
    NearSpanQuery goodFish = new NearSpanQuery(new TermSpanQuery[]{
        new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "good")), fish}, 0, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(goodFish, fishandchips, 3));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  // test with different pre and post, one match in range another not
  @Test
  public void testNotQueryMatchesRangeNotFilterPrePost() throws IOException {
    this.addDocuments(doc(token("fish", node(2)), token("and", node(2)), token("chips", node(2)),
        token("is", node(2)), token("not", node(2)), token("good", node(2)), token("fish", node(2))));
    this.addDocuments(doc(token("good", node(2)), token("fish", node(2)), token("is", node(2)),
        token("not", node(2)), token("fish", node(2)), token("and", node(2)),
        token("chips", node(2))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 1, false);
    NearSpanQuery goodFish = new NearSpanQuery(new TermSpanQuery[]{
        new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "good")), fish}, 0, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(goodFish, fishandchips, 2, 3));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(1, hits.totalHits);
  }

  // exclude in range should filter
  @Test
  public void testNotQueryMatchesRangePrePost() throws IOException {
    this.addDocuments(doc(token("fish", node(2)), token("and", node(2)), token("chips", node(2)),
        token("is", node(2)), token("not", node(2)), token("good", node(2)), token("fish", node(2))));
    this.addDocuments(doc(token("good", node(2)), token("fish", node(2)), token("is", node(2)),
        token("definitely", node(2)), token("not", node(2)), token("fish", node(2)),
        token("and", node(2)), token("chips", node(2))));
    NearSpanQuery fishandchips = new NearSpanQuery(new TermSpanQuery[]{fish, chips}, 1,
        false);
    NearSpanQuery goodFish = new NearSpanQuery(new TermSpanQuery[]{
        new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "good")), fish,}, 0, true);
    Query query = new LuceneProxyNodeQuery(new NotSpanQuery(goodFish, fishandchips, 3, 4));
    TopDocs hits = searcher.search(query, 100);
    assertEquals(0, hits.totalHits);
  }

  @Test
  public void testEquality() throws Exception {
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    NotSpanQuery not1 = new NotSpanQuery(term1, term2);

    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    NotSpanQuery not2 = new NotSpanQuery(term3, term4);

    assertEquals(not1, not2);

    NotSpanQuery not3 = new NotSpanQuery(term3, term4);
    not3.setLevelConstraint(3);
    assertNotEquals(not1, not3);

    NotSpanQuery not4 = new NotSpanQuery(term3, term4);
    not4.setNodeConstraint(5);
    assertNotEquals(not1, not4);
  }

  @Test
  public void testSetLevelConstraint() {
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    NotSpanQuery not1 = new NotSpanQuery(term1, term2);

    not1.setLevelConstraint(3);
    assertEquals(3, not1.getLevelConstraint());
    // Level constraint must have been transferred to the clauses
    assertEquals(3, not1.getInclude().getLevelConstraint());
    assertEquals(3, not1.getExclude().getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "aaa"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "bbb"));
    NotSpanQuery not1 = new NotSpanQuery(term1, term2);

    final TwigQuery twig = new TwigQuery();

    not1.setAncestorPointer(twig);

    assertSame(twig, not1.getAncestorPointer());
    // clauses must have been updated
    assertSame(twig, term1.getAncestorPointer());
  }

}
