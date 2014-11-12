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

import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.util.English;
import org.junit.Test;

import com.sindicetech.siren.analysis.MockSirenDocument;
import com.sindicetech.siren.analysis.MockSirenToken;
import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;
import com.sindicetech.siren.search.spans.NotSpanQuery;
import com.sindicetech.siren.search.spans.OrSpanQuery;
import com.sindicetech.siren.search.spans.PositionRangeSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.util.BasicSirenTestCase;

import java.io.IOException;

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestNodeSpansBasics extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  private MockSirenDocument[] generateDocuments(int n) {
    MockSirenDocument[] docs = new MockSirenDocument[n];
    for (int i = 0; i < n; i++) {
      String values[] = English.intToEnglish(i).trim().split("[\\W]");
      MockSirenToken[] tokens = new MockSirenToken[values.length];
      for (int j = 0; j < values.length; j++) {
        tokens[j] = token(values[j], node(1,j));
      }
      docs[i] = doc(tokens);
    }
    return docs;
  }

  @Test
  public void testSpanNearExact() throws Exception {
    this.addDocuments(this.generateDocuments(500));

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("seventy").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("seven").getQuery());
    NearSpanQuery spanQuery = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);

    this.checkHits(spanQuery, 77, 177, 277, 377, 477);

    QueryUtils.check(term1);
    QueryUtils.check(term2);
    QueryUtils.checkUnequal(term1, term2);
  }

  @Test
  public void testSpanTermQuery() throws Exception {
    this.addDocuments(this.generateDocuments(100));

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("seventy").getQuery());

    checkHits(term1, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79);
  }

  @Test
  public void testSpanNearOrdered() throws Exception {
    this.addDocuments(this.generateDocuments(1000));

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("nine").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("six").getQuery());
    NearSpanQuery query = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 4, true);

    checkHits(query, 906, 926, 936, 946, 956, 966, 976, 986, 996);
  }

  @Test
  public void testSpanNearUnordered() throws Exception {
    this.addDocuments(this.generateDocuments(1000));

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("nine").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("six").getQuery());
    NearSpanQuery query = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 4, false);

    checkHits(query, 609, 629, 639, 649, 659, 669, 679, 689, 699, 906, 926, 936, 946, 956,
                     966, 976, 986, 996);
  }

  @Test
  public void testSpanExactNested() throws Exception {
    this.addDocuments(this.generateDocuments(2000));

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("three").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("hundred").getQuery());
    NearSpanQuery near1 = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    NodeSpanQuery term3 = new NodeSpanQuery(ntq("thirty").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("three").getQuery());
    NearSpanQuery near2 = new NearSpanQuery(new NodeSpanQuery[] {term3, term4}, 0, true);

    NearSpanQuery query = new NearSpanQuery(new SpanQuery[] {near1, near2}, 0, true);

    checkHits(query, new int[] {333, 1333});

    // assertTrue(searcher.explain(query, 333).getValue() > 0.0f);
  }

  @Test
  public void testSpanPositionRange() throws Exception {
    this.addDocuments(this.generateDocuments(600));

    NodeSpanQuery term1 = new NodeSpanQuery(ntq("five").getQuery());
    PositionRangeSpanQuery query = new PositionRangeSpanQuery(term1, 1, 2);

    checkHits(query, new int[] {25, 35, 45, 55, 65, 75, 85, 95});

    //assertTrue(searcher.explain(query, 25).getValue() > 0.0f);
    //assertTrue(searcher.explain(query, 95).getValue() > 0.0f);
    query = new PositionRangeSpanQuery(term1, 6, 7);
    checkHits(query, new int[]{});

    query = new PositionRangeSpanQuery(term1, 0, 1);
    checkHits(query, new int[]
      {5, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512,
              513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525,
              526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538,
              539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551,
              552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564,
              565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577,
              578, 579, 580, 581, 582, 583, 584,
              585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597,
              598, 599});
  }

  @Test
  public void testSpanNot() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("eight").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("one").getQuery());
    NearSpanQuery near = new NearSpanQuery(new SpanQuery[]{term1, term2}, 4, true);
    NodeSpanQuery term3 = new NodeSpanQuery(ntq("forty").getQuery());
    SpanQuery query = new NotSpanQuery(near, term3);

    checkHits(query, new int[]{801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851,
      1861, 1871, 1881, 1891});
  }

  @Test
  public void testSpanNearOr() throws Exception {
    this.addDocuments(this.generateDocuments(2000));

    NodeSpanQuery t1 = new NodeSpanQuery(ntq("six").getQuery());
    NodeSpanQuery t3 = new NodeSpanQuery(ntq("seven").getQuery());

    NodeSpanQuery t5 = new NodeSpanQuery(ntq("seven").getQuery());
    NodeSpanQuery t6 = new NodeSpanQuery(ntq("six").getQuery());

    OrSpanQuery to1 = new OrSpanQuery(t1, t3);
    OrSpanQuery to2 = new OrSpanQuery(t5, t6);

    NearSpanQuery query = new NearSpanQuery(new SpanQuery[] {to1, to2}, 10, true);

    checkHits(query, new int[]
      { 606, 607, 626, 627, 636, 637, 646, 647, 656, 657, 666, 667, 676, 677,
              686, 687, 696, 697, 706, 707, 726, 727, 736, 737, 746, 747, 756,
              757, 766, 767, 776, 777, 786, 787, 796, 797, 1606, 1607, 1626,
              1627, 1636, 1637, 1646, 1647, 1656, 1657, 1666, 1667, 1676, 1677,
              1686, 1687, 1696, 1697, 1706, 1707, 1726, 1727, 1736, 1737,
              1746, 1747, 1756, 1757, 1766, 1767, 1776, 1777, 1786, 1787, 1796,
              1797});
  }

}
