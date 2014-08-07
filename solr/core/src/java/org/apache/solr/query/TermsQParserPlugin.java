package org.apache.solr.query;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Callback;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSetProducer;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.field.NativePagedBytes;
import org.apache.solr.util.CharUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;


public class TermsQParserPlugin extends QParserPlugin {
  public static final String NAME = "terms";

  @Override
  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        final String field = localParams.get(QueryParsing.F);
        final String termStr = localParams.get(QueryParsing.V);

        TFilter tfilter;
        try (TFilter.Builder builder = new TFilter.Builder(field, termStr.length())) {

          CharUtils.splitSmart(termStr, ',', true, new Callback<CharSequence>() {
            final BytesRef br = new BytesRef();
            final SchemaField sf = req.getSchema().getField(field);

            @Override
            public long callback(CharSequence info) {
              sf.getType().readableToIndexed(info, br);
              builder.addTerm(br);
              return 0;
            }
          });

          tfilter = builder.build();

        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }

        return new ConstantScoreQuery(tfilter);
      }
    };
  }
}


class TFilter extends Filter implements DocSetProducer {
  private final String field;
  private final byte[] termBytes;
  private final int nTerms;
  private final int unsorted;
  private int hash;

  private TFilter(String field, byte[] termBytes, int nTerms, int unsorted) {
    this.field = field;
    this.termBytes = termBytes;
    this.nTerms = nTerms;
    this.unsorted = unsorted;
  }


  // if small number of terms, sort?
  // if large number of terms,

  /** Instances of Builder *must* be closed to avoid memory leaks */
  public static class Builder implements Closeable {
    private String field;
    private NativePagedBytes termBytes;
    private BytesRef prev = new BytesRef();
    private int unsorted;
    private int nTerms;

    public Builder(String field, int sizeEstimate) {
      this.field = field;

      int nBits = PackedInts.unsignedBitsRequired((long)sizeEstimate);
      if (nBits < 6)
        nBits = 6;
      else if (nBits > 15) {
        nBits = 15;
      }

      termBytes = new NativePagedBytes(nBits);
    }

    public void addTerm(BytesRef term) {
      // calculate both the shared prefix length, and if the terms are sorted in a single pass

      int checkPrefixLength = Math.min(term.length, prev.length);
      int prefixLen = 0;
      int diff = 0;
      for (; prefixLen<checkPrefixLength; prefixLen++) {
        byte b1 = prev.bytes[prefixLen];
        byte b2 = term.bytes[term.offset + prefixLen];
        diff = b2 - b1;
        if (diff != 0) {
          break;
        }
      }

      if (diff == 0) {
        unsorted += term.length < prev.length ? 1 : 0;
      } else if (diff < 0) {
        unsorted++;
      }

      if (prefixLen >= 0x80 || term.length-prefixLen >= 0x80) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "term exceeded 128 byte limit");
      }

      prev.copyBytes(term);

      if (prefixLen >= 2) {
        termBytes.appendByte((byte)(prefixLen | 0x80));  // high bit is a marker for "prefix length"
        term.offset += prefixLen;  // it should be OK to modify the offset of this BytesRef
        term.length -= prefixLen;
      }

      termBytes.copyUsingLengthPrefix(term);

      nTerms++;
    }

    public byte[] getBytes() {
      return termBytes.buildByteArray();
    }

    public TFilter build() {
      return new TFilter(field, getBytes(), nTerms, unsorted);
    }

    @Override
    public void close() throws IOException {
        if (termBytes != null) {
          termBytes.close();
          termBytes = null;
        }
    }
  }

  public BytesRefIterator iterator() {
    return new BytesIter(termBytes);
  }

  public static class BytesIter implements BytesRefIterator {
    private final byte[] arr;
    private int pos;
    private final BytesRef br = new BytesRef(16);

    public BytesIter(byte[] arr) {
      this.arr = arr;
    }

    @Override
    public BytesRef next() {
      if (pos >= arr.length) return null;

      int len = arr[pos++];
      int prefixLen = 0;
      if ((len & 0x80) != 0) {
        prefixLen = len & 0x7f;
        len = arr[pos++];
      }

      br.length = prefixLen + len;

      if (br.bytes.length < br.length) {
        byte[] newBytes = new byte[prefixLen + len + 8]; // oversize a little here
        System.arraycopy(br.bytes, 0, newBytes, 0, prefixLen); // only copy prefix
        br.bytes = newBytes;
      }

      System.arraycopy(arr, pos, br.bytes, prefixLen, len);
      pos += len;
      return br;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return null;
    }
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    BitDocSetNative resultSet;
    try ( Collector collector = new Collector(context.reader().maxDoc()) ) {
      collect(context, acceptDocs, collector);
      resultSet = collector.takeDocSet();
    }

    SolrRequestInfo.getRequestInfo().addCloseHook(resultSet);

    return resultSet.getDocIdSet();
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = field.hashCode();
      hash = hash * 31 + termBytes.length;
      int len = Math.min(termBytes.length, 128);
      for (int i=0; i<len; i++) {
        hash = hash * 31 + termBytes[i];
      }
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TFilter)) return false;
    TFilter other = (TFilter)o;
    return this.field.equals(other.field) && Arrays.equals(this.termBytes, other.termBytes);
  }



  @Override
  public String toString() {
    return "terms(f=" + field + ",num="+nTerms+",sorted="+(unsorted>0)+",nbytes="+termBytes.length + ")";
  }


  private void collect(AtomicReaderContext readerContext, Bits acceptDocs, Collector collector) throws IOException {
    AtomicReader reader = readerContext.reader();
    Fields fields = reader.fields();
    if (field == null) return;
    Terms terms = fields.terms(field);
    if (terms == null) return;
    TermsEnum tenum = terms.iterator(null);
    DocsEnum docsEnum = null;

    BytesRefIterator iter = iterator();
    for (; ; ) {
      BytesRef term = iter.next();
      if (term == null) break;
      if (tenum.seekExact(term)) {
        docsEnum = tenum.docs(acceptDocs, docsEnum, DocsEnum.FLAG_NONE);
        int id = docsEnum.nextDoc();
        if (id == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        collector.collect(id);
      }
    }
  }

  private static class Collector implements Closeable{
    BitDocSetNative docs;
    int count;
    int maxDoc;
    int base;

    public Collector(int maxDoc) {
      this.maxDoc = maxDoc;
      this.docs = new BitDocSetNative(maxDoc);
    }

    public void collect(int doc) {
      docs.fastSet(doc + base);
      count++;
    }

    public BitDocSetNative takeDocSet() {
      BitDocSetNative ret = docs;
      docs = null;
      return ret;
    }

    @Override
    public void close() throws IOException {
      if (docs != null) {
        docs.close();
      }
    }
  }

  @Override
  public DocSet createDocSet(QueryContext queryContext) throws IOException {
    IndexSearcher searcher = queryContext.searcher();

    try ( Collector collector = new Collector(searcher.getIndexReader().maxDoc()) ) {

      for (AtomicReaderContext readerContext : searcher.getIndexReader().getContext().leaves()) {
        AtomicReader reader = readerContext.reader();
        collector.base = readerContext.docBase;
        collect(readerContext, reader.getLiveDocs(), collector);
      }

      return collector.takeDocSet();
    }
  }


}
