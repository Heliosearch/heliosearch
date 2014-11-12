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
import org.junit.Test;

import com.sindicetech.siren.analysis.MockSirenDocument;
import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.search.node.*;
import com.sindicetech.siren.search.spans.MultiTermSpanQuery;
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.NotSpanQuery;
import com.sindicetech.siren.search.spans.OrSpanQuery;
import com.sindicetech.siren.search.spans.PositionRangeSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.search.spans.TermSpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;

/**
 * Tests for {@link MultiTermSpanQuery}, wrapping a few MultiTermQueries.
 */
public class TestMultiTermSpanQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    setAnalyzer(AnalyzerType.MOCK);
    setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);

    MockSirenDocument mockDoc = doc(token("quick", node(1)), token("brown", node(1)),
        token("fox", node(1)));
    addDocuments(mockDoc);

    mockDoc = doc(token("jumps", node(1)), token("over", node(1)), token("lazy", node(1)),
        token("broun", node(1)), token("dog", node(1)));
    addDocuments(mockDoc);

    mockDoc = doc(token("jumps", node(1)), token("over", node(1)), token("extremely", node(1)),
        token("very", node(1)), token("lazy", node(1)), token("broxn", node(1)),
        token("dog", node(1)));
    addDocuments(mockDoc);
  }

  // public void testWildcard() throws Exception {
  // NodeWildcardQuery wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "bro?n"));
  // TermSpanQuery swq = new MultiTermSpanQuery<NodeWildcardQuery>(wq);
  // // will only match quick brown fox
  // SpanFirstQuery sfq = new SpanFirstQuery(swq, 2);
  // assertEquals(1, searcher.search(sfq, 10).totalHits);
  // }
  //
  // public void testPrefix() throws Exception {
  // NodeWildcardQuery wq = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "extrem*"));
  // TermSpanQuery swq = new MultiTermSpanQuery<NodeWildcardQuery>(wq);
  // // will only match "jumps over extremely very lazy broxn dog"
  // SpanFirstQuery sfq = new SpanFirstQuery(swq, 3);
  // assertEquals(1, searcher.search(sfq, 10).totalHits);
  // }

  public void testFuzzy() throws Exception {
    NodeFuzzyQuery fq = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "broan"));
    SpanQuery sfq = new MultiTermSpanQuery<NodeFuzzyQuery>(fq);
    // will not match quick brown fox
    PositionRangeSpanQuery sprq = new PositionRangeSpanQuery(sfq, 3, 6);
    Query query = new LuceneProxyNodeQuery(sprq);
    assertEquals(2, searcher.search(query, 10).totalHits);
  }

  public void testFuzzy2() throws Exception {
    // maximum of 1 term expansion
    NodeFuzzyQuery fq = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "broan"), 1, 0, 1, false);
    SpanQuery sfq = new MultiTermSpanQuery<NodeFuzzyQuery>(fq);
    // will only match jumps over lazy broun dog
    PositionRangeSpanQuery sprq = new PositionRangeSpanQuery(sfq, 0, 100);
    Query query = new LuceneProxyNodeQuery(sprq);
    assertEquals(1, searcher.search(query, 10).totalHits);
  }

  public void testNoSuchMultiTermsInNear() throws Exception {
    // test to make sure non existent multiterms aren't throwing null pointer exceptions
    NodeFuzzyQuery fuzzyNoSuch = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"), 1, 0,
        1, false);
    SpanQuery spanNoSuch = new MultiTermSpanQuery<NodeFuzzyQuery>(fuzzyNoSuch);
    SpanQuery term = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "brown"));
    SpanQuery near = new NearSpanQuery(new SpanQuery[] { term, spanNoSuch }, 1, true);
    Query query = new LuceneProxyNodeQuery(near);
    assertEquals(0, searcher.search(query, 10).totalHits);
    // flip order
    near = new NearSpanQuery(new SpanQuery[] { spanNoSuch, term }, 1, true);
    query = new LuceneProxyNodeQuery(near);
    assertEquals(0, searcher.search(query, 10).totalHits);

    NodeWildcardQuery wcNoSuch = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "noSuch*"));
    SpanQuery spanWCNoSuch = new MultiTermSpanQuery<NodeWildcardQuery>(wcNoSuch);
    near = new NearSpanQuery(new SpanQuery[] { term, spanWCNoSuch }, 1, true);

    NodeRegexpQuery rgxNoSuch = new NodeRegexpQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
    SpanQuery spanRgxNoSuch = new MultiTermSpanQuery<NodeRegexpQuery>(rgxNoSuch);
    near = new NearSpanQuery(new SpanQuery[] { term, spanRgxNoSuch }, 1, true);
    query = new LuceneProxyNodeQuery(near);
    assertEquals(0, searcher.search(query, 10).totalHits);

    NodePrefixQuery prfxNoSuch = new NodePrefixQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
    SpanQuery spanPrfxNoSuch = new MultiTermSpanQuery<NodePrefixQuery>(prfxNoSuch);
    // test single noSuch
    near = new NearSpanQuery(new SpanQuery[] { spanPrfxNoSuch }, 1, true);
    query = new LuceneProxyNodeQuery(near);
    long actual = searcher.search(query, 10).totalHits;
    assertEquals(0, actual);

    // test double noSuch
    near = new NearSpanQuery(new SpanQuery[] { spanPrfxNoSuch, spanPrfxNoSuch }, 1, true);
    query = new LuceneProxyNodeQuery(near);
    assertEquals(0, searcher.search(query, 10).totalHits);
  }

  public void testNoSuchMultiTermsInNotNear() throws Exception {
    // test to make sure non existent multiterms aren't throwing non-matching field exceptions
    NodeFuzzyQuery fuzzyNoSuch = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"), 1, 0,
        1, false);
    SpanQuery spanNoSuch = new MultiTermSpanQuery<NodeFuzzyQuery>(fuzzyNoSuch);
    SpanQuery term = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "brown"));
    NotSpanQuery notNear = new NotSpanQuery(term, spanNoSuch);
    Query query = new LuceneProxyNodeQuery(notNear);
    assertEquals(1, searcher.search(query, 10).totalHits);

    // flip
    notNear = new NotSpanQuery(spanNoSuch, term);
    query = new LuceneProxyNodeQuery(notNear);
    assertEquals(0, searcher.search(query, 10).totalHits);

    // both noSuch
    notNear = new NotSpanQuery(spanNoSuch, spanNoSuch);
    query = new LuceneProxyNodeQuery(notNear);
    assertEquals(0, searcher.search(query, 10).totalHits);

    NodeWildcardQuery wcNoSuch = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "noSuch*"));
    SpanQuery spanWCNoSuch = new MultiTermSpanQuery<NodeWildcardQuery>(wcNoSuch);
    notNear = new NotSpanQuery(term, spanWCNoSuch);
    query = new LuceneProxyNodeQuery(notNear);
    assertEquals(1, searcher.search(query, 10).totalHits);

    NodeRegexpQuery rgxNoSuch = new NodeRegexpQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
    SpanQuery spanRgxNoSuch = new MultiTermSpanQuery<NodeRegexpQuery>(rgxNoSuch);
    notNear = new NotSpanQuery(term, spanRgxNoSuch, 1, 1);
    query = new LuceneProxyNodeQuery(notNear);
    assertEquals(1, searcher.search(query, 10).totalHits);

    NodePrefixQuery prfxNoSuch = new NodePrefixQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
    SpanQuery spanPrfxNoSuch = new MultiTermSpanQuery<NodePrefixQuery>(prfxNoSuch);
    notNear = new NotSpanQuery(term, spanPrfxNoSuch, 1, 1);
    query = new LuceneProxyNodeQuery(notNear);
    assertEquals(1, searcher.search(query, 10).totalHits);
  }

  public void testNoSuchMultiTermsInOr() throws Exception {
    // test to make sure non existent multiterms aren't throwing null pointer exceptions
    NodeFuzzyQuery fuzzyNoSuch = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"), 1, 0,
        1, false);
    SpanQuery spanNoSuch = new MultiTermSpanQuery<NodeFuzzyQuery>(fuzzyNoSuch);
    SpanQuery term = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "brown"));
    OrSpanQuery near = new OrSpanQuery(new SpanQuery[] { term, spanNoSuch });
    Query query = new LuceneProxyNodeQuery(near);
    assertEquals(1, searcher.search(query, 10).totalHits);

    // flip
    near = new OrSpanQuery(new SpanQuery[] { spanNoSuch, term });
    query = new LuceneProxyNodeQuery(near);
    assertEquals(1, searcher.search(query, 10).totalHits);
    NodeWildcardQuery wcNoSuch = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "noSuch*"));
    SpanQuery spanWCNoSuch = new MultiTermSpanQuery<NodeWildcardQuery>(wcNoSuch);
    near = new OrSpanQuery(new SpanQuery[] { term, spanWCNoSuch });
    query = new LuceneProxyNodeQuery(near);
    assertEquals(1, searcher.search(query, 10).totalHits);

    NodeRegexpQuery rgxNoSuch = new NodeRegexpQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
    SpanQuery spanRgxNoSuch = new MultiTermSpanQuery<NodeRegexpQuery>(rgxNoSuch);
    near = new OrSpanQuery(new SpanQuery[] { term, spanRgxNoSuch });
    query = new LuceneProxyNodeQuery(near);
    assertEquals(1, searcher.search(query, 10).totalHits);

    NodePrefixQuery prfxNoSuch = new NodePrefixQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
    SpanQuery spanPrfxNoSuch = new MultiTermSpanQuery<NodePrefixQuery>(prfxNoSuch);
    near = new OrSpanQuery(new SpanQuery[] { term, spanPrfxNoSuch });
    query = new LuceneProxyNodeQuery(near);
    assertEquals(1, searcher.search(query, 10).totalHits);

    near = new OrSpanQuery(new SpanQuery[] { spanPrfxNoSuch });
    query = new LuceneProxyNodeQuery(near);
    assertEquals(0, searcher.search(query, 10).totalHits);

    near = new OrSpanQuery(new SpanQuery[] { spanPrfxNoSuch, spanPrfxNoSuch });
    query = new LuceneProxyNodeQuery(near);
    assertEquals(0, searcher.search(query, 10).totalHits);
  }

  // public void testNoSuchMultiTermsInSpanFirst() throws Exception {
  // //this hasn't been a problem
  // NodeFuzzyQuery fuzzyNoSuch = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"), 1, 0,
  // 1, false);
  // SpanQuery spanNoSuch = new MultiTermSpanQuery<NodeFuzzyQuery>(fuzzyNoSuch);
  // SpanQuery spanFirst = new SpanFirstQuery(spanNoSuch, 10);
  //
  // assertEquals(0, searcher.search(spanFirst, 10).totalHits);
  //
  // NodeWildcardQuery wcNoSuch = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "noSuch*"));
  // SpanQuery spanWCNoSuch = new MultiTermSpanQuery<NodeWildcardQuery>(wcNoSuch);
  // spanFirst = new SpanFirstQuery(spanWCNoSuch, 10);
  // assertEquals(0, searcher.search(spanFirst, 10).totalHits);
  //
  // NodeRegexpQuery rgxNoSuch = new NodeRegexpQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
  // SpanQuery spanRgxNoSuch = new MultiTermSpanQuery<NodeRegexpQuery>(rgxNoSuch);
  // spanFirst = new SpanFirstQuery(spanRgxNoSuch, 10);
  // assertEquals(0, searcher.search(spanFirst, 10).totalHits);
  //
  // NodePrefixQuery prfxNoSuch = new NodePrefixQuery(new Term(DEFAULT_TEST_FIELD, "noSuch"));
  // SpanQuery spanPrfxNoSuch = new MultiTermSpanQuery<NodePrefixQuery>(prfxNoSuch);
  // spanFirst = new SpanFirstQuery(spanPrfxNoSuch, 10);
  // assertEquals(0, searcher.search(spanFirst, 10).totalHits);
  // }

  @Test
  public void testEquality() throws Exception {
    MultiTermSpanQuery term1 = new MultiTermSpanQuery(new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "aaa*")));
    MultiTermSpanQuery term2 = new MultiTermSpanQuery(new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "bbb*")));

    assertNotEquals(term1, term2);

    MultiTermSpanQuery term3 = new MultiTermSpanQuery(new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "aaa*")));

    assertEquals(term1, term3);
  }

  @Test
  public void testSetLevelConstraint() {
    NodeWildcardQuery term1 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "aaa*"));
    MultiTermSpanQuery mtsq = new MultiTermSpanQuery(term1);

    mtsq.setLevelConstraint(3);
    assertEquals(3, mtsq.getLevelConstraint());
    // Level constraint must have been transferred to the clauses
    assertEquals(3, mtsq.query.getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    NodeWildcardQuery term1 = new NodeWildcardQuery(new Term(DEFAULT_TEST_FIELD, "aaa*"));
    MultiTermSpanQuery mtsq = new MultiTermSpanQuery(term1);

    final TwigQuery twig = new TwigQuery();

    mtsq.setAncestorPointer(twig);

    assertSame(twig, mtsq.getAncestorPointer());
    // clauses must have been updated
    assertSame(twig, term1.getAncestorPointer());
  }

}