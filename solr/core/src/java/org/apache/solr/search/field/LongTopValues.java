package org.apache.solr.search.field;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.core.HS;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;

public class LongTopValues extends TopValues {

  public LongTopValues(LongFieldValues longFieldValues) {
    super(longFieldValues);
  }

  protected static class Uninvert extends LeafValues.Uninvert {
    private boolean first = true;
    LongFieldStats stats = new LongFieldStats();

    long currentValue;
    private final FieldCache.LongParser parser = FieldCache.NUMERIC_UTILS_LONG_PARSER;

    long arr;

    public Uninvert(AtomicReaderContext readerContext, SchemaField field) {
      super(readerContext, field);
    }

    @Override
    public void visitTerm(BytesRef term) {
      currentValue = parser.parseLong(term);
      if (first) {
        first = false;
        stats.firstValue = currentValue;
        arr = HS.allocArray(maxDoc, 8, true);
      }
    }

    @Override
    public void visitDoc(int docID) {
      HS.setLong(arr, docID, currentValue);
    }

    @Override
    protected TermsEnum termsEnum(Terms terms) throws IOException {
      return parser.termsEnum(terms);
    }

    protected void done() {
      stats.lastValue = currentValue;
      stats.numUniqueValues = super.termNum;
      stats.numDocsWithField = super.termsDocCount;
    }

    @Override
    public void close() throws IOException {
      if (arr != 0) {
        HS.freeArray(arr);
        arr = 0;
      }
      super.close();
    }
  }

  @Override
  public LongLeafValues createValue(QueryContext context, CreationLeafValue create, AtomicReaderContext readerContext) throws IOException {
    try (
        Uninvert u = new Uninvert(readerContext, fieldValues.field);
    ) {
      u.uninvert();

      long arr = u.arr;
      BitDocSetNative docsWithField = u.docsWithField;
      u.docsWithField = null;

      if (arr == 0) {
        return new Long0LeafValues(fieldValues);
      }

      long minValue = u.stats.getFirst();
      long maxValue = u.stats.getLast();
      long minNeeded = Math.min(minValue, 0);
      long maxNeeded = Math.max(maxValue, 0);
      long range = maxNeeded - minNeeded;
      if (range < 0) range=Long.MAX_VALUE;  // overflow... MAXLONG-MINLONG

      if (range < 256) {  // endpoints are inclusive, so we need less-than (i.e. 0..2 == range of 2 == 3 distinct values)
        long offset = 128 + minNeeded;
        // future: use 0 offset if possible
        // if (minValue >= -128 && maxValue <= 127) {
        //   offset = 0;
        // }

        long arr2 = HS.allocArray(u.maxDoc, 1, false);
        for (int i=0; i<u.maxDoc; i++) {
          byte v = (byte)(HS.getLong(arr, i) - offset);
          HS.setByte(arr2, i, v);
          // assert HS.getInt(arr,i) == offset + HS.getByte(arr2, i);
        }
        return new Long8LeafValues(fieldValues, arr2, offset, docsWithField, u.stats);
      } else if (range < 65536) {
        long offset = 32768 + minNeeded;
        // future: use 0 offset if possible
        // if (minValue >= -32768 && maxValue <= 32767) {
        //  offset = 0;
        // }
        long arr2 = HS.allocArray(u.maxDoc, 2, false);
        for (int i=0; i<u.maxDoc; i++) {
          short v = (short)(HS.getLong(arr, i) - offset);
          HS.setShort(arr2, i, v);
        }
        return new Long16LeafValues(fieldValues, arr2, offset, docsWithField, u.stats);
      } else if (range < 0x0000000100000000L) {
        long offset = 0x0000000080000000L + minNeeded;
        // future: use 0 offset if possible
        // if (minValue >= -32768 && maxValue <= 32767) {
        //  offset = 0;
        // }
        long arr2 = HS.allocArray(u.maxDoc, 4, false);
        for (int i=0; i<u.maxDoc; i++) {
          int v = (int)(HS.getLong(arr, i) - offset);
          HS.setInt(arr2, i, v);
        }
        return new Long32LeafValues(fieldValues, arr2, offset, docsWithField, u.stats);
      } else {
        // steal the array
        u.arr = 0;
        return new Long64LeafValues(fieldValues, arr, docsWithField, u.stats);
      }
    }

  }

  @Override
  public LongTopValues create(SolrIndexSearcher.WarmContext warmContext) {
    LongTopValues tv = new LongTopValues((LongFieldValues)fieldValues);
    tv.create(warmContext, this);
    return tv;
  }
}
