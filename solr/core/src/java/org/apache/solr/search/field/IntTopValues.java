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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.core.HS;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;

public class IntTopValues extends TopValues {

  public IntTopValues(IntFieldValues intFieldValues) {
    super(intFieldValues);
  }

  protected static class IntUninvert extends LeafValues.Uninvert {
    private boolean first = true;
    IntFieldStats stats = new IntFieldStats();

    int currentValue;
    private final FieldCache.IntParser parser = FieldCache.NUMERIC_UTILS_INT_PARSER;

    long arr;

    public IntUninvert(AtomicReaderContext readerContext, SchemaField field) {
      super(readerContext, field);
    }

    @Override
    public void visitTerm(BytesRef term) {
      currentValue = parser.parseInt(term);
      if (first) {
        first = false;
        stats.firstValue = currentValue;
        arr = HS.allocArray(maxDoc, 4, true);
      }
    }

    @Override
    public void visitDoc(int docID) {
      HS.setInt(arr, docID, currentValue);
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
  public IntLeafValues createValue(TopValues topValues, CreationLeafValue create, AtomicReaderContext readerContext) throws IOException {
    try (
        IntUninvert u = new IntUninvert(readerContext, fieldValues.field);
    ) {
      u.uninvert();

      // TODO: if we could get the max value ahead of time, we could avoid
      // creating the wrong size array in the first place...

      long arr = u.arr;  // TODO: change to getters?
      BitDocSetNative docsWithField = u.docsWithField;
      u.docsWithField = null;

      if (arr == 0) {
        return new Int0LeafValues(fieldValues);
      }

      int minValue = u.stats.getFirst();
      int maxValue = u.stats.getLast();
      int minNeeded = Math.min(minValue, 0);  // TODO: we currently need to encode 0 for "missing"... would be better to encode it as minValue-1 to create a contiguous space...
      int maxNeeded = Math.max(maxValue, 0);
      long range = (long)maxNeeded - minNeeded;



      if (range < 256) {  // endpoints are inclusive, so we need less-than (i.e. 0..2 == range of 2 == 3 distinct values)
        int offset = 128 + minNeeded;
        // future: use 0 offset if possible
        // if (minValue >= -128 && maxValue <= 127) {
        //   offset = 0;
        // }

        long arr2 = HS.allocArray(u.maxDoc, 1, false);
        for (int i=0; i<u.maxDoc; i++) {
          byte v = (byte)(HS.getInt(arr, i) - offset);
          HS.setByte(arr2, i, v);
          // assert HS.getInt(arr,i) == offset + HS.getByte(arr2, i);
        }
        return new Int8LeafValues(fieldValues, arr2, offset, docsWithField, u.stats);
      } else if (range < 65536) {
        int offset = 32768 + minNeeded;
        // future: use 0 offset if possible
        // if (minValue >= -32768 && maxValue <= 32767) {
        //  offset = 0;
        // }
        long arr2 = HS.allocArray(u.maxDoc, 2, false);
        for (int i=0; i<u.maxDoc; i++) {
          short v = (short)(HS.getInt(arr, i) - offset);
          HS.setShort(arr2, i, v);
          // assert HS.getInt(arr,i) == offset + HS.getShort(arr2, i);
        }
        return new Int16LeafValues(fieldValues, arr2, offset, docsWithField, u.stats);
      } else {
        // steal the array
        u.arr = 0;
        return new Int32LeafValues(fieldValues, arr, docsWithField, u.stats);
      }
    }

  }

  @Override
  public IntTopValues create(SolrIndexSearcher.WarmContext warmContext) {
    IntTopValues tv = new IntTopValues((IntFieldValues)fieldValues);
    tv.create(warmContext, this);
    return tv;
  }
}
