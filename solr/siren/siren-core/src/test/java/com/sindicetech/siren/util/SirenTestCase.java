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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.analysis.*;
import com.sindicetech.siren.analysis.AnyURIAnalyzer.URINormalisation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class SirenTestCase extends LuceneTestCase {

  protected static final Logger logger = LoggerFactory.getLogger(SirenTestCase.class);

  public static final String DEFAULT_TEST_FIELD = "content";

  public static Analyzer newTupleAnalyzer() {
    final AnyURIAnalyzer uriAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    uriAnalyzer.setUriNormalisation(URINormalisation.FULL);
    final TupleAnalyzer analyzer = new TupleAnalyzer(TEST_VERSION_CURRENT,
      new StandardAnalyzer(TEST_VERSION_CURRENT), uriAnalyzer);
    return analyzer;
  }

  public static Analyzer newJsonAnalyzer() {
    final AnyURIAnalyzer fieldAnalyzer = new AnyURIAnalyzer(TEST_VERSION_CURRENT);
    fieldAnalyzer.setUriNormalisation(URINormalisation.FULL);
    final Analyzer literalAnalyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);
    final ExtendedJsonAnalyzer analyzer = new ExtendedJsonAnalyzer(fieldAnalyzer, literalAnalyzer);
    return analyzer;
  }

  public static Analyzer newMockAnalyzer() {
    return new MockSirenAnalyzer();
  }

  private static FieldType newFieldType() {
    final FieldType ft = new FieldType();
    ft.setStored(false);
    ft.setOmitNorms(false);
    ft.setIndexed(true);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    ft.setTokenized(true);
    return ft;
  }

  protected static FieldType newStoredFieldType() {
    final FieldType ft = newFieldType();
    ft.setStored(true);
    return ft;
  }

  private FieldType newStoredNoNormFieldType() {
    final FieldType ft = newStoredFieldType();
    ft.setOmitNorms(true);
    return ft;
  }

  protected static RandomIndexWriter newRandomIndexWriter(final Directory dir,
                                                          final Analyzer analyzer,
                                                          final Codec codec)
  throws IOException {
    return newRandomIndexWriter(dir, analyzer, codec,
      newIndexWriterConfig(analyzer)
        .setCodec(codec).setMergePolicy(newLogMergePolicy())
        .setSimilarity(new DefaultSimilarity()));
  }

  protected static RandomIndexWriter newRandomIndexWriter(final Directory dir,
                                                          final Analyzer analyzer,
                                                          final Codec codec,
                                                          final IndexWriterConfig config)
  throws IOException {
    final RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    writer.setDoRandomForceMergeAssert(true);
    return writer;
  }

  protected static IndexReader newIndexReader(final RandomIndexWriter writer)
  throws IOException {
    // We are wrapping by default the reader into a slow reader, as most of the
    // tests require an atomic reader
    return SlowCompositeReaderWrapper.wrap(writer.getReader());
  }

  /**
   * Create a new searcher over the reader. This searcher might randomly use
   * threads.
   * <p>
   * Override the original {@link LuceneTestCase#newSearcher(IndexReader)}
   * implementation in order to avoid getting {@link org.apache.lucene.search.AssertingIndexSearcher}
   * which is incompatible with SIREn.
   * <p>
   * TODO: Implement our own {@link AssertingIndexSearcher} and {@link org.apache.lucene.search.AssertingScorer}
   */
  public static IndexSearcher newSearcher(final IndexReader r) {
    final Random random = random();
    if (usually()) {
      // compared to the original implementation, we do not wrap to avoid
      // wrapping into an AssertingAtomicReader
      return random.nextBoolean() ? new IndexSearcher(r) : new IndexSearcher(r.getContext());
    } else {
      int threads = 0;
      final ThreadPoolExecutor ex;
      if (random.nextBoolean()) {
        ex = null;
      } else {
        threads = TestUtil.nextInt(random, 1, 8);
        ex = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("LuceneTestCase"));
      }
      if (ex != null) {
       if (VERBOSE) {
        System.out.println("NOTE: newSearcher using ExecutorService with " + threads + " threads");
       }
       r.addReaderClosedListener(new ReaderClosedListener() {
         @Override
         public void onClose(final IndexReader reader) {
           TestUtil.shutdownExecutorService(ex);
         }
       });
      }
      final IndexSearcher ret = random.nextBoolean()
          ? new IndexSearcher(r, ex)
          : new IndexSearcher(r.getContext(), ex);
      return ret;
    }
  }

  protected static void addDocument(final RandomIndexWriter writer, final String data)
  throws IOException {
    final Document doc = new Document();
    doc.add(new Field(DEFAULT_TEST_FIELD, data, newStoredFieldType()));
    writer.addDocument(doc);
    writer.commit();
  }

  protected void addDocumentNoNorms(final RandomIndexWriter writer, final String data)
  throws IOException {
    final Document doc = new Document();
    doc.add(new Field(DEFAULT_TEST_FIELD, data, this.newStoredNoNormFieldType()));
    writer.addDocument(doc);
    writer.commit();
  }

  /**
   * Atomically adds a block of documents with sequentially
   * assigned document IDs.
   * <br>
   * See also {@link IndexWriter#addDocuments(Iterable)}
   */
  protected static void addDocuments(final RandomIndexWriter writer,
                                     final String[] data)
  throws IOException {
    final ArrayList<Document> docs = new ArrayList<Document>();

    for (final String entry : data) {
      final Document doc = new Document();
      doc.add(new Field(DEFAULT_TEST_FIELD, entry, newStoredFieldType()));
      docs.add(doc);
    }
    writer.addDocuments(docs);
    writer.commit();
  }

  protected static void addDocuments(final RandomIndexWriter writer,
                                     final MockSirenDocument ... sdocs)
  throws IOException {
    final ArrayList<Document> docs = new ArrayList<Document>(sdocs.length);
    for (final MockSirenDocument sdoc : sdocs) {
      final Document doc = new Document();
      doc.add(new Field(DEFAULT_TEST_FIELD, new MockSirenReader(sdoc), newFieldType()));
      docs.add(doc);
    }
    writer.addDocuments(docs);
    writer.commit();
  }

  protected void deleteAll(final RandomIndexWriter writer) throws IOException {
    writer.deleteAll();
    writer.commit();
  }

  protected void forceMerge(final RandomIndexWriter writer) throws IOException {
    writer.forceMerge(1);
  }

}
