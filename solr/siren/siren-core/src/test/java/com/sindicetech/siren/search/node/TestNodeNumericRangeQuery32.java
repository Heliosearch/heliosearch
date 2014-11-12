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

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.FloatNumericAnalyzer;
import com.sindicetech.siren.analysis.IntNumericAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.util.SirenTestCase;
import com.sindicetech.siren.util.XSDDatatype;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.junit.*;

import java.io.IOException;
import java.util.Random;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.BooleanClauseBuilder.must;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeNumericRangeQueryBuilder.nmqFloat;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeNumericRangeQueryBuilder.nmqInt;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.TwigChildBuilder.child;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.TwigQueryBuilder.twq;
import static com.sindicetech.siren.search.AbstractTestSirenScorer.dq;

public class TestNodeNumericRangeQuery32 extends SirenTestCase {

  private final float[] FLOAT_NANs =  {
    Float.NaN,
    Float.intBitsToFloat(0x7f800001),
    Float.intBitsToFloat(0x7fffffff),
    Float.intBitsToFloat(0xff800001),
    Float.intBitsToFloat(0xffffffff)
  };

  private final Random random = new Random(random().nextLong());
  // distance of entries
  private static final int distance = 6666;
  // shift the starting of the values to the left, to also have negative values:
  private static final int startOffset = - 1 << 15;
  // number of docs to generate for testing
  private static int noDocs;

  private static Index index;

  private static class Index {
    Directory directory = null;
    IndexReader reader = null;
    IndexSearcher searcher = null;
    RandomIndexWriter writer = null;
  }

  private static void init(final Index index)
  throws IOException {
    final RandomSirenCodec codec = new RandomSirenCodec(random(), PostingsFormatType.RANDOM);
    final TupleAnalyzer tupleAnalyzer = (TupleAnalyzer) SirenTestCase.newTupleAnalyzer();
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    tupleAnalyzer.registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);

    // Set the SIREn fields
    codec.addSirenFields("field8", "field4", "field2", "field" + Integer.MAX_VALUE,
      "ascfield8", "ascfield4", "ascfield2",
      "float8", "float4", "float2");
    // Set the datatype analyzers
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_INT+"8").toCharArray(), new IntNumericAnalyzer(8));
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_INT+"4").toCharArray(), new IntNumericAnalyzer(4));
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_INT+"2").toCharArray(), new IntNumericAnalyzer(2));
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_FLOAT+"8").toCharArray(), new FloatNumericAnalyzer(8));
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_FLOAT+"4").toCharArray(), new FloatNumericAnalyzer(4));
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_FLOAT+"2").toCharArray(), new FloatNumericAnalyzer(2));
    tupleAnalyzer.registerDatatype((XSDDatatype.XSD_INT+Integer.MAX_VALUE).toCharArray(), new IntNumericAnalyzer(Integer.MAX_VALUE));

    index.directory = newDirectory();
    index.writer = newRandomIndexWriter(index.directory, tupleAnalyzer, codec);
  }

  @BeforeClass
  public static void setUpBeforeClass()
  throws IOException {
    index = new Index();
    init(index);

    // Add a series of noDocs docs with increasing int values
    noDocs = atLeast(4096);
    for (int l = 0; l < noDocs; l++) {
      final Document doc = new Document();

      int val = distance * l + startOffset;

      // add fields, that have a distance to test general functionality
      doc.add(new Field("field8", getTriple(val, XSDDatatype.XSD_INT+"8"), newStoredFieldType()));
      doc.add(new Field("field4", getTriple(val, XSDDatatype.XSD_INT+"4"), newStoredFieldType()));
      doc.add(new Field("field2", getTriple(val, XSDDatatype.XSD_INT+"2"), newStoredFieldType()));
      doc.add(new Field("field"+Integer.MAX_VALUE, getTriple(val, XSDDatatype.XSD_INT+Integer.MAX_VALUE), newStoredFieldType()));

      // add ascending fields with a distance of 1, beginning at -noDocs/2 to
      // test the correct splitting of range and inclusive/exclusive
      val = l - (noDocs / 2);

      doc.add(new Field("ascfield8", getTriple(val, XSDDatatype.XSD_INT+"8"), newStoredFieldType()));
      doc.add(new Field("ascfield4", getTriple(val, XSDDatatype.XSD_INT+"4"), newStoredFieldType()));
      doc.add(new Field("ascfield2", getTriple(val, XSDDatatype.XSD_INT+"2"), newStoredFieldType()));

      doc.add(new Field("float8", getTriple(val, XSDDatatype.XSD_FLOAT+"8"), newStoredFieldType()));
      doc.add(new Field("float4", getTriple(val, XSDDatatype.XSD_FLOAT+"4"), newStoredFieldType()));
      doc.add(new Field("float2", getTriple(val, XSDDatatype.XSD_FLOAT+"2"), newStoredFieldType()));

      index.writer.addDocument(doc);
    }
    index.writer.commit();
    index.reader = newIndexReader(index.writer);
    index.searcher = newSearcher(index.reader);
  }

  @AfterClass
  public static void tearDownAfterClass()
  throws IOException {
    if (index != null) {
      close(index);
    }
  }

  private static void close(final Index index)
  throws IOException {
    if (index.searcher != null) {
      index.searcher = null;
    }
    if (index.reader != null) {
      index.reader.close();
      index.reader = null;
    }
    if (index.writer != null) {
      index.writer.close();
      index.writer = null;
    }
    if (index.directory != null) {
      index.directory.close();
      index.directory = null;
    }
  }

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    // Remove maximum clause limit for the tests
    NodeBooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
  }

  @Override
  @After
  public void tearDown()
  throws Exception {
    super.tearDown();
  }

  private static String getTriple(final Number val, final String datatypeURI) {
    return "<http://fake.subject> <http://fake.predicate/"+val+"> \"" + val + "\"^^<"+datatypeURI+"> .\n";
  }

  private static String getLiteralValue(final String triple) {
    final int firstColon = triple.indexOf('"');
    final int secondColon = triple.indexOf('"', firstColon + 1);
    return triple.substring(firstColon + 1, secondColon);
  }

  /** test for both constant score and boolean query, the other tests only use the constant score mode */
  private void testRange(final int precisionStep) throws Exception {
    final String field="field"+precisionStep;
    final int count=3000;
    final int lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);

    final Query dq = twq(1)
    .with(child(must(nmqInt(field, precisionStep, lower, upper, true, true)
    .setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE)
    .bound(2, 2)))).getLuceneProxyQuery();

    TopDocs topDocs;
    String type;

    type = " (constant score boolean rewrite)";
    topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);

    final ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count"+type, count, sd.length);
    Document doc=index.searcher.doc(sd[0].doc);
    assertEquals("First doc"+type, 2*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
    doc=index.searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc"+type, (1+count)*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
  }

  @Test
  public void testRange_8bit() throws Exception {
    this.testRange(8);
  }

  @Test
  public void testRange_4bit() throws Exception {
    this.testRange(4);
  }

  @Test
  public void testRange_2bit() throws Exception {
    this.testRange(2);
  }

  @Test
  public void testInverseRange() throws Exception {
    Query dq = twq(1)
    .with(child(must(nmqInt("field8", 8, 1000, -1000, true, true)
    .setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE))))
    .getLuceneProxyQuery();

    TopDocs topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    assertEquals("A inverse range should return the EMPTY_DOCIDSET instance", 0, topDocs.totalHits);

    dq = twq(1)
    .with(child(must(nmqInt("field8", 8, Integer.MAX_VALUE, null, false, false)
    .setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE))))
    .getLuceneProxyQuery();
    topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    assertEquals("A exclusive range starting with Integer.MAX_VALUE should return the EMPTY_DOCIDSET instance", 0, topDocs.totalHits);

    dq = twq(1)
    .with(child(must(nmqInt("field8", 8, null, Integer.MIN_VALUE, false, false)
    .setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE))))
    .getLuceneProxyQuery();
    topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    assertEquals("A exclusive range ending with Integer.MIN_VALUE should return the EMPTY_DOCIDSET instance", 0, topDocs.totalHits);
  }

  @Test
  public void testOneMatchQuery() throws Exception {
    final Query dq = twq(1)
    .with(child(must(nmqInt("ascfield8", 8, 1000, 1000, true, true)
    .setRewriteMethod(MultiNodeTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE))))
    .getLuceneProxyQuery();
    final TopDocs topDocs = index.searcher.search(dq, noDocs);
    final ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", 1, sd.length );
  }

  private void testLeftOpenRange(final int precisionStep) throws Exception {
    final String field="field"+precisionStep;
    final int count=3000;
    final int upper=(count-1)*distance + (distance/3) + startOffset;

    Query dq = twq(1)
    .with(child(must(nmqInt(field, precisionStep, null, upper, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();

    TopDocs topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    Document doc = index.searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
    doc=index.searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));

    dq = twq(1)
    .with(child(must(nmqInt(field, precisionStep, null, upper, false, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    doc=index.searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
    doc=index.searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
  }

  @Test
  public void testLeftOpenRange_8bit() throws Exception {
    this.testLeftOpenRange(8);
  }

  @Test
  public void testLeftOpenRange_4bit() throws Exception {
    this.testLeftOpenRange(4);
  }

  @Test
  public void testLeftOpenRange_2bit() throws Exception {
    this.testLeftOpenRange(2);
  }

  private void testRightOpenRange(final int precisionStep) throws Exception {
    final String field="field"+precisionStep;
    final int count=3000;
    final int lower=(count-1)*distance + (distance/3) +startOffset;

    Query dq = twq(1)
    .with(child(must(nmqInt(field, precisionStep, lower, null, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();

    TopDocs topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length);
    Document doc = index.searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
    doc = index.searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));

    dq = twq(1)
    .with(child(must(nmqInt(field, precisionStep, lower, null, true, false)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(dq, null, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    doc = index.searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
    doc = index.searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, Integer.parseInt(getLiteralValue(doc.get(field))));
  }

  @Test
  public void testRightOpenRange_8bit() throws Exception {
    this.testRightOpenRange(8);
  }

  @Test
  public void testRightOpenRange_4bit() throws Exception {
    this.testRightOpenRange(4);
  }

  @Test
  public void testRightOpenRange_2bit() throws Exception {
    this.testRightOpenRange(2);
  }

  @Test
  public void testInfiniteValues() throws Exception {
    final Index index = new Index();

    init(index);

    Document doc = new Document();
    doc.add(new Field("float4", getTriple(Float.NEGATIVE_INFINITY, XSDDatatype.XSD_FLOAT+"4"), newStoredFieldType()));
    doc.add(new Field("field4", getTriple(Integer.MIN_VALUE, XSDDatatype.XSD_INT+"4"), newStoredFieldType()));
    index.writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("float4", getTriple(Float.POSITIVE_INFINITY, XSDDatatype.XSD_FLOAT+"4"), newStoredFieldType()));
    doc.add(new Field("field4", getTriple(Integer.MAX_VALUE, XSDDatatype.XSD_INT+"4"), newStoredFieldType()));
    index.writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("float4", getTriple(0.0f, XSDDatatype.XSD_FLOAT+"4"), newStoredFieldType()));
    doc.add(new Field("field4", getTriple(0, XSDDatatype.XSD_INT+"4"), newStoredFieldType()));
    index.writer.addDocument(doc);

    for (final float f : FLOAT_NANs) {
      doc = new Document();
      doc.add(new Field("float4", getTriple(f, XSDDatatype.XSD_FLOAT+"4"), newStoredFieldType()));
      index.writer.addDocument(doc);
    }
    index.writer.commit();
    index.reader = newIndexReader(index.writer);
    index.searcher = newSearcher(index.reader);

    Query q = twq(1)
      .with(child(must(nmqInt("field4", 4, null, null, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    TopDocs topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqInt("field4", 4, null, null, false, false)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqInt("field4", 4, Integer.MIN_VALUE, Integer.MAX_VALUE, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqInt("field4", 4, Integer.MIN_VALUE, Integer.MAX_VALUE, false, false)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqInt("field4", 4, null, null, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqFloat("float4", 4, null, null, false, false)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqFloat("float4", 4, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqFloat("float4", 4, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, false, false)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q = twq(1)
      .with(child(must(nmqFloat("float4", 4, Float.NaN, Float.NaN, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    topDocs = index.searcher.search(q, 10);
    assertEquals("Score doc count", FLOAT_NANs.length,  topDocs.scoreDocs.length );

    close(index);
  }

  private void testRandomTrieAndClassicRangeQuery(final int precisionStep) throws Exception {
    final String field="field"+precisionStep;
    int totalTermCountT=0,totalTermCountC=0,termCountT,termCountC;
    final int num = TestUtil.nextInt(random(), 10, 20);

    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    for (int i = 0; i < num; i++) {
      int lower=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      int upper=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        final int a=lower; lower=upper; upper=a;
      }
      /*
       * In SIREn, the numeric type and the precision step are prepended to the
       * indexed numeric terms.
       */
      final BytesRefBuilder lowerBytes = new BytesRefBuilder();
      lowerBytes.append(new BytesRef(FieldType.NumericType.INT.toString() + precisionStep));
      final BytesRefBuilder upperBytes = new BytesRefBuilder();
      upperBytes.append(new BytesRef(FieldType.NumericType.INT.toString() + precisionStep));
      final BytesRefBuilder lBytes = new BytesRefBuilder();
      final BytesRefBuilder uBytes = new BytesRefBuilder();
      NumericUtils.intToPrefixCoded(lower, 0, lBytes);
      NumericUtils.intToPrefixCoded(upper, 0, uBytes);
      lowerBytes.append(lBytes);
      upperBytes.append(uBytes);

      // test inclusive range
      MultiNodeTermQuery tq = (MultiNodeTermQuery) nmqInt(field, precisionStep, lower, upper, true, true).getQuery();
      MultiNodeTermQuery cq = new NodeTermRangeQuery(field, lowerBytes.get(), upperBytes.get(), true, true);
      TopDocs tTopDocs = index.searcher.search(dq(tq), 1);
      TopDocs cTopDocs = index.searcher.search(dq(cq), 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = this.countTerms(tq);
      totalTermCountC += termCountC = this.countTerms(cq);
      this.checkTermCounts(precisionStep, termCountT, termCountC);
      // test exclusive range
      tq = (MultiNodeTermQuery) nmqInt(field, precisionStep, lower, upper, false, false).getQuery();
      cq=new NodeTermRangeQuery(field, lowerBytes.get(), upperBytes.get(), false, false);
      tTopDocs = index.searcher.search(dq(tq), 1);
      cTopDocs = index.searcher.search(dq(cq), 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = this.countTerms(tq);
      totalTermCountC += termCountC = this.countTerms(cq);
      this.checkTermCounts(precisionStep, termCountT, termCountC);
      // test left exclusive range
      tq=(MultiNodeTermQuery) nmqInt(field, precisionStep, lower, upper, false, true).getQuery();
      cq=new NodeTermRangeQuery(field, lowerBytes.get(), upperBytes.get(), false, true);
      tTopDocs = index.searcher.search(dq(tq), 1);
      cTopDocs = index.searcher.search(dq(cq), 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = this.countTerms(tq);
      totalTermCountC += termCountC = this.countTerms(cq);
      this.checkTermCounts(precisionStep, termCountT, termCountC);
      // test right exclusive range
      tq=(MultiNodeTermQuery) nmqInt(field, precisionStep, lower, upper, true, false).getQuery();
      cq=new NodeTermRangeQuery(field, lowerBytes.get(), upperBytes.get(), true, false);
      tTopDocs = index.searcher.search(dq(tq), 1);
      cTopDocs = index.searcher.search(dq(cq), 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = this.countTerms(tq);
      totalTermCountC += termCountC = this.countTerms(cq);
      this.checkTermCounts(precisionStep, termCountT, termCountC);
    }

    this.checkTermCounts(precisionStep, totalTermCountT, totalTermCountC);
    if (VERBOSE && precisionStep != Integer.MAX_VALUE) {
      System.out.println("Average number of terms during random search on '" + field + "':");
      System.out.println(" Numeric query: " + (((double)totalTermCountT)/(num * 4)));
      System.out.println(" Classical query: " + (((double)totalTermCountC)/(num * 4)));
    }
  }

  private void checkTermCounts(final int precisionStep, final int termCountT, final int termCountC) {
    if (precisionStep == Integer.MAX_VALUE) {
      assertEquals("Number of terms should be equal for unlimited precStep", termCountC, termCountT);
    } else {
      assertTrue("Number of terms for NRQ should be <= compared to classical TRQ", termCountT <= termCountC);
    }
  }

  @Test
  public void testRandomTrieAndClassicRangeQuery_8bit() throws Exception {
    this.testRandomTrieAndClassicRangeQuery(8);
  }

  @Test
  public void testRandomTrieAndClassicRangeQuery_4bit() throws Exception {
    this.testRandomTrieAndClassicRangeQuery(4);
  }

  @Test
  public void testRandomTrieAndClassicRangeQuery_2bit() throws Exception {
    this.testRandomTrieAndClassicRangeQuery(2);
  }

  @Test
  public void testRandomTrieAndClassicRangeQuery_NoTrie() throws Exception {
    this.testRandomTrieAndClassicRangeQuery(Integer.MAX_VALUE);
  }

  private void testRangeSplit(final int precisionStep) throws Exception {
    final String field="ascfield"+precisionStep;
    // 10 random tests
    final int num = 10 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) {
      int lower=(int)(random.nextDouble()*noDocs - noDocs/2);
      int upper=(int)(random.nextDouble()*noDocs - noDocs/2);
      if (lower>upper) {
        final int a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      Query dq = twq(1)
      .with(child(must(nmqInt(field, precisionStep, lower, upper, true, true)
        .bound(2, 2)))).getLuceneProxyQuery();
      TopDocs tTopDocs = index.searcher.search(dq, 1);
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
      // test exclusive range
      dq = twq(1)
      .with(child(must(nmqInt(field, precisionStep, lower, upper, false, false)
        .bound(2, 2)))).getLuceneProxyQuery();
      tTopDocs = index.searcher.search(dq, 1);
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits );
      // test left exclusive range
      dq = twq(1)
      .with(child(must(nmqInt(field, precisionStep, lower, upper, false, true)
        .bound(2, 2)))).getLuceneProxyQuery();
      tTopDocs = index.searcher.search(dq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
      // test right exclusive range
      dq = twq(1)
      .with(child(must(nmqInt(field, precisionStep, lower, upper, true, false)
        .bound(2, 2)))).getLuceneProxyQuery();
      tTopDocs = index.searcher.search(dq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
    }
  }

  @Test
  public void testRangeSplit_8bit() throws Exception {
    this.testRangeSplit(8);
  }

  @Test
  public void testRangeSplit_4bit() throws Exception {
    this.testRangeSplit(4);
  }

  @Test
  public void testRangeSplit_2bit() throws Exception {
    this.testRangeSplit(2);
  }

  /** we fake a float test using int2float conversion of NumericUtils */
  private void testFloatRange(final int precisionStep) throws Exception {
    final String field="float"+precisionStep;
    final float lower=-1000, upper=+2000;

//    final Query tq=SirenNumericRangeQuery.newFloatRange(field, precisionStep,
//      NumericUtils.sortableIntToFloat(lower), NumericUtils.sortableIntToFloat(upper), true, true);

    /*
     * Original Lucene test was faking a float using the NumericUtils.sortableIntToFloat method.
     * Since in Siren we index also the datatype, we cannot do that: using a float query to search
     * for a value indexed with XSD_INT datatype.
     */
    final Query dq = twq(1)
    .with(child(must(nmqFloat(field, precisionStep, lower, upper, true, true)
      .bound(2, 2)))).getLuceneProxyQuery();
    final TopDocs tTopDocs = index.searcher.search(dq, 1);
    assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits, 0);
  }

  @Test
  public void testFloatRange_8bit() throws Exception {
    this.testFloatRange(8);
  }

  @Test
  public void testFloatRange_4bit() throws Exception {
    this.testFloatRange(4);
  }

  @Test
  public void testFloatRange_2bit() throws Exception {
    this.testFloatRange(2);
  }

  @Test
  public void testEqualsAndHash() throws Exception {
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test1", 4, 10, 20, true, true));
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test2", 4, 10, 20, false, true));
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test3", 4, 10, 20, true, false));
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test4", 4, 10, 20, false, false));
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test5", 4, 10, null, true, true));
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test6", 4, null, 20, true, true));
    QueryUtils.checkHashEquals(NodeNumericRangeQuery.newIntRange("test7", 4, null, null, true, true));
    QueryUtils.checkEqual(
      NodeNumericRangeQuery.newIntRange("test8", 4, 10, 20, true, true),
      NodeNumericRangeQuery.newIntRange("test8", 4, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      NodeNumericRangeQuery.newIntRange("test9", 4, 10, 20, true, true),
      NodeNumericRangeQuery.newIntRange("test9", 8, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      NodeNumericRangeQuery.newIntRange("test10a", 4, 10, 20, true, true),
      NodeNumericRangeQuery.newIntRange("test10b", 4, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      NodeNumericRangeQuery.newIntRange("test11", 4, 10, 20, true, true),
      NodeNumericRangeQuery.newIntRange("test11", 4, 20, 10, true, true)
    );
    QueryUtils.checkUnequal(
      NodeNumericRangeQuery.newIntRange("test12", 4, 10, 20, true, true),
      NodeNumericRangeQuery.newIntRange("test12", 4, 10, 20, false, true)
    );
    QueryUtils.checkUnequal(
      NodeNumericRangeQuery.newIntRange("test13", 4, 10, 20, true, true),
      NodeNumericRangeQuery.newFloatRange("test13", 4, 10f, 20f, true, true)
    );
    // the following produces a hash collision, because Long and Integer have the same hashcode, so only test equality:
    final Query q1 = NodeNumericRangeQuery.newIntRange("test14", 4, 10, 20, true, true);
    final Query q2 = NodeNumericRangeQuery.newLongRange("test14", 4, 10L, 20L, true, true);
    assertFalse(q1.equals(q2));
    assertFalse(q2.equals(q1));
  }

  @Test
  public void testEmptyEnums() throws Exception {
    final int count=3000;
    int lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    // test empty enum
    assert lower < upper;
    assertTrue(0 < this.countTerms((MultiNodeTermQuery) nmqInt("field4", 4, lower, upper, true, true).getQuery()));
    assertEquals(0, this.countTerms((MultiNodeTermQuery) nmqInt("field4", 4, upper, lower, true, true).getQuery()));
    // test empty enum outside of bounds
    lower = distance*noDocs+startOffset;
    upper = 2 * lower;
    assert lower < upper;
    assertEquals(0, this.countTerms((MultiNodeTermQuery) nmqInt("field4", 4, lower, upper, true, true).getQuery()));
  }

  private int countTerms(final MultiNodeTermQuery q) throws Exception {
    final Terms terms = MultiFields.getTerms(index.reader, q.getField());
    if (terms == null)
      return 0;
    final TermsEnum termEnum = q.getTermsEnum(terms);
    assertNotNull(termEnum);
    int count = 0;
    BytesRef cur, last = null;
    while ((cur = termEnum.next()) != null) {
      count++;
      if (last != null) {
        assertTrue(last.compareTo(cur) < 0);
      }
      last = BytesRef.deepCopyOf(cur);
    }
    // LUCENE-3314: the results after next() already returned null are undefined,
    // assertNull(termEnum.next());
    return count;
  }

  // TODO: Investigate how to set a custom ordering of terms
//  private void testSorting(int precisionStep) throws Exception {
//    String field="field"+precisionStep;
//    // 10 random tests, the index order is ascending,
//    // so using a reverse sort field should retun descending documents
//    int num = _TestUtil.nextInt(random(), 10, 20);
//    for (int i = 0; i < num; i++) {
//      int lower=(int)(random().nextDouble()*noDocs*distance)+startOffset;
//      int upper=(int)(random().nextDouble()*noDocs*distance)+startOffset;
//      if (lower>upper) {
//        int a=lower; lower=upper; upper=a;
//      }
//      Query dq = twq(1)
//      .with(child(must(nmqInt(field, precisionStep, lower, upper, true, true)
//        .bound(2, 2)))).getDocumentQuery();
//      TopDocs topDocs = index.searcher.search(dq, null, noDocs, new Sort(new SortField(field, SortField.Type.INT, true)));
//      if (topDocs.totalHits==0) continue;
//      ScoreDoc[] sd = topDocs.scoreDocs;
//      assertNotNull(sd);
//      int last = index.searcher.doc(sd[0].doc).getField(field).numericValue().intValue();
//      for (int j=1; j<sd.length; j++) {
//        int act = index.searcher.doc(sd[j].doc).getField(field).numericValue().intValue();
//        assertTrue("Docs should be sorted backwards", last>act );
//        last=act;
//      }
//    }
//  }
//
//  @Test
//  public void testSorting_8bit() throws Exception {
//    testSorting(8);
//  }
//
//  @Test
//  public void testSorting_4bit() throws Exception {
//    testSorting(4);
//  }
//
//  @Test
//  public void testSorting_2bit() throws Exception {
//    testSorting(2);
//  }

}