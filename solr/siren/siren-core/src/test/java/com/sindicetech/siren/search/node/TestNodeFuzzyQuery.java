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

import static com.sindicetech.siren.search.AbstractTestSirenScorer.dq;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import com.sindicetech.siren.analysis.AnyURIAnalyzer;
import com.sindicetech.siren.analysis.TupleAnalyzer;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeFuzzyQuery;
import com.sindicetech.siren.search.node.MultiNodeTermQuery.TopTermsBoostOnlyNodeBooleanQueryRewrite;
import com.sindicetech.siren.util.BasicSirenTestCase;
import com.sindicetech.siren.util.XSDDatatype;

/**
 * Tests copied from {@link FuzzyQuery} for the siren use case.
 */
public class TestNodeFuzzyQuery extends BasicSirenTestCase {

  @Override
  protected void configure() throws IOException {
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    final TupleAnalyzer tupleAnalyzer = new TupleAnalyzer(TEST_VERSION_CURRENT,
      new WhitespaceAnalyzer(TEST_VERSION_CURRENT), uriAnalyzer);
    tupleAnalyzer.registerDatatype(XSDDatatype.XSD_ANY_URI.toCharArray(), uriAnalyzer);
    this.setAnalyzer(tupleAnalyzer);
    this.setPostingsFormat(PostingsFormatType.RANDOM);
  }

  @Test
  public void testFuzziness() throws Exception {
    this.addDocument("<aaaaa>");
    this.addDocument("<aaaab>");
    this.addDocument("<aaabb>");
    this.addDocument("<aabbb>");
    this.addDocument("<abbbb>");
    this.addDocument("<bbbbb>");
    this.addDocument("<ddddd>");

    LuceneProxyNodeQuery query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 0));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);

    // same with prefix
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 1));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 2));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 3));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 4));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 5));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 6));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // test scoring
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "bbbbb"), NodeFuzzyQuery.defaultMaxEdits, 0));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("3 documents should match", 3, hits.length);
    List<Integer> order = Arrays.asList(5,4,3);
    for (int i = 0; i < hits.length; i++) {
      assertEquals((int) order.get(i), hits[i].doc);
    }

    // test pq size by supplying maxExpansions=2
    // This query would normally return 3 documents, because 3 terms match (see above):
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "bbbbb"), NodeFuzzyQuery.defaultMaxEdits, 0, 2, false));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("only 2 documents should match", 2, hits.length);
    order = Arrays.asList(5,4);
    for (int i = 0; i < hits.length; i++) {
      assertEquals((int) order.get(i), hits[i].doc);
    }

    // not similar enough:
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "xxxxx"), NodeFuzzyQuery.defaultMaxEdits, 0));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaccc"), NodeFuzzyQuery.defaultMaxEdits, 0));   // edit distance to "aaaaa" = 3
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // query identical to a word in the index:
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaaa"), NodeFuzzyQuery.defaultMaxEdits, 0));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(0, hits[0].doc);
    // default allows for up to two edits:
    assertEquals(1, hits[1].doc);
    assertEquals(2, hits[2].doc);

    // query similar to a word in the index:
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaac"), NodeFuzzyQuery.defaultMaxEdits, 0));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(0, hits[0].doc);
    assertEquals(1, hits[1].doc);
    assertEquals(2, hits[2].doc);

    // now with prefix
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaac"), NodeFuzzyQuery.defaultMaxEdits, 1));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(0, hits[0].doc);
    assertEquals(1, hits[1].doc);
    assertEquals(2, hits[2].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaac"), NodeFuzzyQuery.defaultMaxEdits, 2));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(0, hits[0].doc);
    assertEquals(1, hits[1].doc);
    assertEquals(2, hits[2].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaac"), NodeFuzzyQuery.defaultMaxEdits, 3));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(0, hits[0].doc);
    assertEquals(1, hits[1].doc);
    assertEquals(2, hits[2].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaac"), NodeFuzzyQuery.defaultMaxEdits, 4));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    assertEquals(0, hits[0].doc);
    assertEquals(1, hits[1].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "aaaac"), NodeFuzzyQuery.defaultMaxEdits, 5));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);


    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 0));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(6, hits[0].doc);

    // now with prefix
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 1));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(6, hits[0].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 2));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(6, hits[0].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 3));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(6, hits[0].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 4));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(6, hits[0].doc);
    query = dq(new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 5));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);


    // different field = no match:
    query = dq(new NodeFuzzyQuery(new Term("anotherfield", "ddddX"), NodeFuzzyQuery.defaultMaxEdits, 0));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
  }

  /**
   * MultiTermQuery provides (via attribute) information about which values
   * must be competitive to enter the priority queue.
   *
   * FuzzyQuery optimizes itself around this information, if the attribute
   * is not implemented correctly, there will be problems!
   */
  public void testTieBreaker() throws Exception {
    this.addDocument("<a123456>");
    this.addDocument("<c123456>");
    this.addDocument("<d123456>");
    this.addDocument("<e123456>");

    final Directory directory2 = newDirectory();
    final RandomIndexWriter writer2 = newRandomIndexWriter(directory2, analyzer, codec);
    addDocument(writer2, "<a123456>");
    addDocument(writer2, "<b123456>");
    addDocument(writer2, "<b123456>");
    addDocument(writer2, "<b123456>");
    addDocument(writer2, "<c123456>");
    addDocument(writer2, "<f123456>");

    final IndexReader ir1 = writer.getReader();
    final IndexReader ir2 = writer2.getReader();

    final MultiReader mr = new MultiReader(ir1, ir2);
    final IndexSearcher searcher = newSearcher(mr);
    final FuzzyQuery fq = new FuzzyQuery(new Term(DEFAULT_TEST_FIELD, "z123456"), 1, 0, 2, false);
    final TopDocs docs = searcher.search(fq, 2);
    assertEquals(5, docs.totalHits); // 5 docs, from the a and b's

    mr.close();
    ir2.close();
    writer2.close();
    directory2.close();
  }

  /** Test the {@link TopTermsBoostOnlyNodeBooleanQueryRewrite} rewrite method. */
  @Test
  public void testBoostOnlyRewrite() throws Exception {
    this.addDocument("<Lucene>");
    this.addDocument("<Lucene>");
    this.addDocument("<Lucenne>");

    final NodeFuzzyQuery query = new NodeFuzzyQuery(new Term(DEFAULT_TEST_FIELD, "Lucene"));
    query.setRewriteMethod(new MultiNodeTermQuery.TopTermsBoostOnlyNodeBooleanQueryRewrite(50));
    final ScoreDoc[] hits = searcher.search(dq(query), null, 1000).scoreDocs;
    assertEquals(3, hits.length);
  }

}
