/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TrieField;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.NumericDocValues;

import org.apache.lucene.util.BytesRef;

import org.apache.solr.common.util.NamedList;

public class HashQParserPlugin extends QParserPlugin {

  public void init(NamedList params) {

  }

  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
    return new HashQParser(query, localParams, params, request);
  }

  private class HashQParser extends QParser {

    public HashQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
      super(query, localParams, params, request);
    }

    public Query parse() {
      int workers = localParams.getInt("workers");
      int worker = localParams.getInt("worker");
      String keys = localParams.get("keys");
      return new HashQuery(keys, workers, worker);
    }
  }

  private class HashQuery extends ExtendedQueryBase implements PostFilter {

    private String keysParam;
    private int workers;
    private int worker;

    public HashQuery(String keysParam, int workers, int worker) {
      this.keysParam = keysParam;
      this.workers = workers;
      this.worker = worker;
    }

    public DelegatingCollector getFilterCollector(IndexSearcher indexSearcher) {
      String[] keys = keysParam.split(",");
      HashKey[] hashKeys = new HashKey[keys.length];
      SolrIndexSearcher searcher = (SolrIndexSearcher)indexSearcher;
      IndexSchema schema = searcher.getSchema();
      for(int i=0; i<keys.length; i++) {
        String key = keys[i];
        FieldType ft = schema.getField(key).getType();
        HashKey h = null;
        if(ft instanceof StrField) {
          h = new BytesHash(key);
        } else {
          h = new NumericHash(key);
        }
        hashKeys[i] = h;
      }
      HashKey k = (hashKeys.length > 1) ? new CompositeHash(hashKeys) : hashKeys[0];
      return new HashCollector(k, workers, worker);
    }
  }

  private class HashCollector extends DelegatingCollector {
    private int worker;
    private int workers;
    private HashKey hashKey;

    public HashCollector(HashKey hashKey, int workers, int worker) {
      this.hashKey = hashKey;
      this.workers = workers;
      this.worker = worker;
    }

    public void setScorer(Scorer scorer) throws IOException{
      delegate.setScorer(scorer);
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.hashKey.setNextReader(context);
      delegate.setNextReader(context);
    }

    public void collect(int doc) throws IOException {
      if(hashKey.hashCode() % workers == worker) {
        delegate.collect(doc);
      }
    }
  }

  private interface HashKey {
    public void setNextReader(AtomicReaderContext reader) throws IOException;
    public long hashCode(int doc);
  }

  private class BytesHash implements HashKey {

    private SortedDocValues values;
    private String field;

    public BytesHash(String field) {
      this.field = field;
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      values = context.reader().getSortedDocValues(field);
    }

    public long hashCode(int doc) {
      BytesRef ref = values.get(doc);
      return ref.hashCode();
    }
  }

  private class NumericHash implements HashKey {

    private NumericDocValues values;
    private String field;

    public NumericHash(String field) {
      this.field = field;
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      values = context.reader().getNumericDocValues(field);
    }

    public long hashCode(int doc) {
      long l = values.get(doc);
      return l;
    }
  }

  private class ZeroHash implements HashKey {

    public long hashCode(int doc) {
      return 0;
    }

    public void setNextReader(AtomicReaderContext context) {

    }
  }

  private class CompositeHash implements HashKey {

    private HashKey key1;
    private HashKey key2;
    private HashKey key3;
    private HashKey key4;

    public CompositeHash(HashKey[] hashKeys) {
      key1 = hashKeys[0];
      key2 = hashKeys[1];
      key3 = (hashKeys.length > 2) ? hashKeys[2] : new ZeroHash();
      key4 = (hashKeys.length > 3) ? hashKeys[3] : new ZeroHash();
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      key1.setNextReader(context);
      key2.setNextReader(context);
      key3.setNextReader(context);
      key4.setNextReader(context);
    }

    public long hashCode(int doc) {
      return key1.hashCode(doc)+key2.hashCode(doc)+key3.hashCode(doc)+key4.hashCode(doc);
    }
  }
}
