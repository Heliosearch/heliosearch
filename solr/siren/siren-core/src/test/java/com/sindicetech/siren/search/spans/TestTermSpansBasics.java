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
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.util.English;
import org.junit.Test;

import com.sindicetech.siren.analysis.MockSirenDocument;
import com.sindicetech.siren.analysis.MockSirenToken;
import com.sindicetech.siren.index.codecs.RandomSirenCodec;
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

public class TestTermSpansBasics extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    this.setAnalyzer(AnalyzerType.MOCK);
    this.setPostingsFormat(RandomSirenCodec.PostingsFormatType.RANDOM);
  }

  private MockSirenDocument[] generateDocuments(int howMany) {
    return generateDocuments(0, howMany);
  }

  private MockSirenDocument[] generateDocuments(int start, int end) {
    assertTrue(end>=start);
    int n = end-start+1;
    MockSirenDocument[] docs = new MockSirenDocument[n];
    for (int i = start; i <= end; i++) {
      String values[] = English.intToEnglish(i).trim().split("[\\W]+");
      MockSirenToken[] tokens = new MockSirenToken[values.length];
      for (int j = 0; j < values.length; j++) {
        tokens[j] = token(values[j], node(1));
      }
      docs[i-start] = doc(tokens);
    }
    return docs;
  }

  @Test
  public void testSpanNearExact() throws Exception {
    this.addDocuments(this.generateDocuments(500));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "seventy"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "seven"));
    NearSpanQuery spanQuery = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);

    this.checkHits(spanQuery, 77, 177, 277, 377, 477);

    QueryUtils.check(term1);
    QueryUtils.check(term2);
    QueryUtils.checkUnequal(term1, term2);
  }

  @Test
  public void testSpanTermQuery() throws Exception {
    this.addDocuments(this.generateDocuments(100));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "seventy"));

    checkHits(term1, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79);
  }

  @Test
  public void testSpanNearOrdered() throws Exception {
    this.addDocuments(this.generateDocuments(1000));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "nine"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "six"));
    NearSpanQuery query = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 4, true);

    checkHits(query, 906, 926, 936, 946, 956, 966, 976, 986, 996);
  }

  @Test
  public void testSpanNearUnordered() throws Exception {
    this.addDocuments(this.generateDocuments(1000));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "nine"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "six"));
    NearSpanQuery query = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 4, false);

    checkHits(query, 609, 629, 639, 649, 659, 669, 679, 689, 699, 906, 926, 936, 946, 956,
                     966, 976, 986, 996);
  }

  @Test
  public void testSpanExactNested() throws Exception {
    this.addDocuments(this.generateDocuments(2000));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "hundred"));
    NearSpanQuery near1 = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "thirty"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    NearSpanQuery near2 = new NearSpanQuery(new TermSpanQuery[] {term3, term4}, 0, true);

    NearSpanQuery query = new NearSpanQuery(new SpanQuery[] {near1, near2}, 0, true);

    checkHits(query, new int[] {333, 1333});

    // assertTrue(searcher.explain(query, 333).getValue() > 0.0f);
  }

  @Test
  public void testSpanPositionRange() throws Exception {
    this.addDocuments(this.generateDocuments(600));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "five"));
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
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    SpanQuery query = new NotSpanQuery(near, term3);

    checkHits(query, new int[]{801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851,
        1861, 1871, 1881, 1891});
  }

  @Test
  public void testSpanWithMultipleNotSingle() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));

    SpanQuery or = new OrSpanQuery(new TermSpanQuery[]{term3});

    SpanQuery query = new NotSpanQuery(near, or);

    checkHits(query, new int[]{801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851,
        1861, 1871, 1881, 1891});
  }

  @Test
  public void testSpanWithMultipleNotMany() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "sixty"));
    TermSpanQuery term5 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eighty"));

    OrSpanQuery or = new OrSpanQuery(new TermSpanQuery[]{term3, term4, term5});

    NotSpanQuery query = new NotSpanQuery(near, or);

    checkHits(query, new int[]{801, 821, 831, 851, 871, 891, 1801, 1821, 1831, 1851, 1871, 1891});

  }

  @Test
  public void testNpeInSpanNearWithSpanNot() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery hun = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "hundred"));
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    NearSpanQuery exclude = new NearSpanQuery(new TermSpanQuery[]{hun, term3}, 1, true);

    NotSpanQuery query = new NotSpanQuery(near, exclude);

    checkHits(query, new int[]{801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851,
        1861, 1871, 1881, 1891});

  }

  //TODO when TermSpanFirstQuery is available
  /*
   * @Test public void testNpeInSpanNearInSpanFirstInSpanNot() throws Exception {
   * this.addDocuments(this.generateDocuments(2000)); int n = 5; TermSpanQuery hun = new
   * TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "hundred")); TermSpanQuery term40 = new
   * TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty")); NearSpanQuery term40c =
   * (NearSpanQuery)term40.clone();
   *
   * SpanFirstQuery include = new SpanFirstQuery(term40, n); NearSpanQuery near = new
   * NearSpanQuery(new TermSpanQuery[]{hun, term40c}, n-1, true); SpanFirstQuery exclude = new
   * SpanFirstQuery(near, n-1); NotSpanQuery q = new NotSpanQuery(include, exclude);
   *
   * checkHits(q, new int[]{40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 1040, 1041, 1042, 1043, 1044,
   * 1045, 1046, 1047, 1048, 1049, 1140, 1141, 1142, 1143, 1144, 1145, 1146, 1147, 1148, 1149, 1240,
   * 1241, 1242, 1243, 1244, 1245, 1246, 1247, 1248, 1249, 1340, 1341, 1342, 1343, 1344, 1345, 1346,
   * 1347, 1348, 1349, 1440, 1441, 1442, 1443, 1444, 1445, 1446, 1447, 1448, 1449, 1540, 1541, 1542,
   * 1543, 1544, 1545, 1546, 1547, 1548, 1549, 1640, 1641, 1642, 1643, 1644, 1645, 1646, 1647, 1648,
   * 1649, 1740, 1741, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 1840, 1841, 1842, 1843, 1844,
   * 1845, 1846, 1847, 1848, 1849, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949}); }
   */

  @Test
  public void testSpanNotWindowOne() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    SpanQuery query = new NotSpanQuery(near, term3, 1, 1);

    checkHits(query, new int[]{840, 842, 843, 844, 845, 846, 847, 848, 849, 1840, 1842, 1843, 1844,
        1845, 1846, 1847, 1848, 1849});

  }

  @Test
  public void testSpanNotWindowTwoBefore() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    NotSpanQuery query = new NotSpanQuery(near, term3, 2, 0);

    checkHits(query, new int[]{840, 841, 842, 843, 844, 845, 846, 847, 848, 849});
  }

  @Test
  public void testSpanNotWindowNeg() throws Exception {
    this.addDocuments(this.generateDocuments(2000));
    // test handling of invalid window < 0
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "eight"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 4, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));

    OrSpanQuery or = new OrSpanQuery(new TermSpanQuery[]{term3});

    NotSpanQuery query = new NotSpanQuery(near, or);

    checkHits(query, new int[]{801, 821, 831, 851, 861, 871, 881, 891, 1801, 1821, 1831, 1851,
        1861, 1871, 1881, 1891});
  }

  @Test
  public void testSpanNotWindowDoubleExcludesBefore() throws Exception {
    // test hitting two excludes before an include
    this.addDocuments(this.generateDocuments(2000));
    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "two"));
    NearSpanQuery near = new NearSpanQuery(new TermSpanQuery[]{term1, term2}, 2, true);
    TermSpanQuery exclude = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "one"));

    SpanQuery query = new NotSpanQuery(near, exclude, 4, 1);

    checkHits(query, new int[]{42, 242, 342, 442, 542, 642, 742, 842, 942});
  }

  @Test
  public void testSpanOr() throws Exception {
    this.addDocuments(this.generateDocuments(2000));

    TermSpanQuery term1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "thirty"));
    TermSpanQuery term2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "three"));
    NearSpanQuery near1 = new NearSpanQuery(new TermSpanQuery[] {term1, term2}, 0, true);
    TermSpanQuery term3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "forty"));
    TermSpanQuery term4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD, "seven"));
    NearSpanQuery near2 = new NearSpanQuery(new TermSpanQuery[] {term3, term4}, 0, true);

    OrSpanQuery query = new OrSpanQuery(near1, near2);

    checkHits(query, new int[]
            { 33, 47, 133, 147, 233, 247, 333, 347, 433, 447, 533, 547, 633, 647, 733,
              747, 833, 847, 933, 947, 1033, 1047, 1133, 1147, 1233, 1247, 1333,
              1347, 1433, 1447, 1533, 1547, 1633, 1647, 1733, 1747, 1833, 1847, 1933, 1947});
  }

  @Test
  public void testSpanNearOr() throws Exception {
    this.addDocuments(this.generateDocuments(2000));

    TermSpanQuery t1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"six"));
    TermSpanQuery t3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"seven"));

    TermSpanQuery t5 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"seven"));
    TermSpanQuery t6 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"six"));

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

  @Test
  public void testSpanComplex1() throws Exception {
    this.addDocuments(this.generateDocuments(2000));

    TermSpanQuery t1 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"six"));
    TermSpanQuery t2 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"hundred"));
    NearSpanQuery tt1 = new NearSpanQuery(new TermSpanQuery[] {t1, t2}, 0, true);

    TermSpanQuery t3 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"seven"));
    TermSpanQuery t4 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"hundred"));
    NearSpanQuery tt2 = new NearSpanQuery(new TermSpanQuery[] {t3, t4}, 0, true);

    TermSpanQuery t5 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"seven"));
    TermSpanQuery t6 = new TermSpanQuery(new Term(DEFAULT_TEST_FIELD,"six"));

    OrSpanQuery to1 = new OrSpanQuery(tt1, tt2);
    OrSpanQuery to2 = new OrSpanQuery(t5, t6);

    NearSpanQuery query = new NearSpanQuery(new SpanQuery[] {to1, to2}, 100, true);

    checkHits(query, new int[]
            { 606, 607, 626, 627, 636, 637, 646, 647, 656, 657, 666, 667, 676, 677, 686, 687, 696,
              697, 706, 707, 726, 727, 736, 737, 746, 747, 756, 757,
              766, 767, 776, 777, 786, 787, 796, 797, 1606, 1607, 1626, 1627, 1636, 1637, 1646,
              1647, 1656, 1657,
              1666, 1667, 1676, 1677, 1686, 1687, 1696, 1697, 1706, 1707, 1726, 1727, 1736, 1737,
              1746, 1747, 1756, 1757, 1766, 1767, 1776, 1777, 1786, 1787, 1796, 1797});
  }

}
