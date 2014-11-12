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
package com.sindicetech.siren.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;

import com.sindicetech.siren.analysis.MockSirenDocument;
import com.sindicetech.siren.index.codecs.RandomSirenCodec;
import com.sindicetech.siren.index.codecs.RandomSirenCodec.PostingsFormatType;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeQuery;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.List;

public abstract class BasicSirenTestCase extends SirenTestCase {

  protected Directory directory;
  protected RandomIndexWriter writer;
  protected IndexReader reader;
  protected IndexSearcher searcher;
  protected Analyzer analyzer;
  protected RandomSirenCodec codec;
  private IndexWriterConfig config;

  public enum AnalyzerType {
    MOCK, TUPLE, JSON
  }

  /**
   * Default configuration for the tests.
   * <p>
   * Overrides must call {@link #setAnalyzer(AnalyzerType)} and
   * {@link #setPostingsFormat(PostingsFormatType)} or
   * {@link #setPostingsFormat(PostingsFormat)}
   */
  protected abstract void configure() throws IOException;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.configure();
  }

  private void init() throws IOException {
    directory = newDirectory();
    if (config == null) {
      writer = newRandomIndexWriter(directory, analyzer, codec);
    } else {
      writer = newRandomIndexWriter(directory, analyzer, codec, config);
    }
    this.deleteAll(writer);
    reader = newIndexReader(writer);
    searcher = newSearcher(reader);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    this.close();
    config = null;
    super.tearDown();
  }

  private void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (directory != null) {
      directory.close();
      directory = null;
    }
  }

  protected void setIndexWriterConfig(final IndexWriterConfig config)
  throws IOException {
    this.config = config;
    if (analyzer != null || codec != null) {
      this.close();
      this.init();
    }
  }

  /**
   * Set a new postings format for a single test
   */
  protected void setPostingsFormat(final PostingsFormatType format)
  throws IOException {
    codec = new RandomSirenCodec(random(), format);
    if (analyzer != null) {
      this.close();
      this.init();
    }
  }

  /**
   * Set a new postings format for a single test
   * @throws IOException
   */
  protected void setPostingsFormat(final PostingsFormat format)
  throws IOException {
    codec = new RandomSirenCodec(random(), format);
    if (analyzer != null) {
      this.close();
      this.init();
    }
  }

  /**
   * Set a new analyzer for a single test
   */
  protected void setAnalyzer(final AnalyzerType analyzerType) throws IOException {
    this.analyzer = this.initAnalyzer(analyzerType);
    if (codec != null) {
      this.close();
      this.init();
    }
  }

  /**
   * Set a new analyzer for a single test
   */
  protected void setAnalyzer(final Analyzer analyzer) throws IOException {
    this.analyzer = analyzer;
    if (codec != null) {
      this.close();
      this.init();
    }
  }

  protected void refreshReaderAndSearcher() throws IOException {
    reader.close();
    reader = newIndexReader(writer);
    searcher = newSearcher(reader);
  }

  protected void addDocument(final String data)
  throws IOException {
    addDocument(writer, data);
    this.refreshReaderAndSearcher();
  }

  protected void addDocument(final Document doc)
  throws IOException {
    writer.addDocument(doc);
    writer.commit();
    this.refreshReaderAndSearcher();
  }

  protected void addDocuments(final List<Document> docs)
  throws IOException {
    writer.addDocuments(docs);
    writer.commit();
    this.refreshReaderAndSearcher();
  }

  protected void addDocumentNoNorms(final String data)
  throws IOException {
    this.addDocumentNoNorms(writer, data);
    this.refreshReaderAndSearcher();
  }

  protected void addDocuments(final Collection<String> docs)
  throws IOException {
    addDocuments(writer, docs.toArray(new String[docs.size()]));
    this.refreshReaderAndSearcher();
  }

  protected void addDocuments(final String ... docs)
  throws IOException {
    addDocuments(writer, docs);
    this.refreshReaderAndSearcher();
  }

  protected void addDocuments(final MockSirenDocument ... sdocs)
  throws IOException {
    addDocuments(writer, sdocs);
    this.refreshReaderAndSearcher();
  }

  protected void deleteAll() throws IOException {
    this.deleteAll(writer);
    this.refreshReaderAndSearcher();
  }

  public void forceMerge() throws IOException {
    this.forceMerge(writer);
    this.refreshReaderAndSearcher();
  }

  private Analyzer initAnalyzer(final AnalyzerType analyzerType) {
    switch (analyzerType) {
      case MOCK:
        return SirenTestCase.newMockAnalyzer();

      case TUPLE:
        return SirenTestCase.newTupleAnalyzer();

      case JSON:
        return SirenTestCase.newJsonAnalyzer();

      default:
        throw new InvalidParameterException();
    }
  }

  protected void checkHits(Query query, int ... results) throws IOException {
    if (query instanceof NodeQuery) {
      query = new LuceneProxyNodeQuery((NodeQuery) query);
    }

    // Use index order to ensure that the scoring does not influence the order
    TopDocs hits = searcher.search(query, 1000, Sort.INDEXORDER);

    assertEquals(results.length, hits.totalHits);

    for (int i = 0; i < results.length; i++) {
      assertEquals(results[i], hits.scoreDocs[i].doc);
    }
  }

}
