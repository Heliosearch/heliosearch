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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.core.HS;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;

public class StrTopValues extends TopValues {

  public StrTopValues(StrFieldValues StrFieldValues) {
    super(StrFieldValues);
  }


  @Override
  public StrLeafValues createValue(TopValues topValues, CreationLeafValue create, AtomicReaderContext readerContext) throws IOException {
    AtomicReader reader = readerContext.reader();
    final int maxDoc = readerContext.reader().maxDoc();

    Terms terms = reader.terms(topValues.fieldValues.getFieldName());

    if (terms == null) {
      return new Str0Values(topValues.fieldValues, new StrFieldStats());
    }

    StrFieldStats stats = new StrFieldStats();

    NativePagedBytes bytes = new NativePagedBytes(15);
    LongArray docToOrd = null;
    long termBytes = 0;


    final int termCountHardLimit;
    if (maxDoc >= Integer.MAX_VALUE - 1) {
      termCountHardLimit = Integer.MAX_VALUE - 1;
    } else {
      termCountHardLimit = maxDoc + 1;
    }


    long numUniqueTerms = terms.size();
    if (numUniqueTerms != -1L) {    // TODO: which codecs don't provide the number of terms???
      if (numUniqueTerms > termCountHardLimit) {
        // app is misusing the API (there is more than
        // one term per doc); in this case we make best
        // effort to load what we can (see LUCENE-2142)
        numUniqueTerms = termCountHardLimit;
      }
    } else {
      numUniqueTerms = termCountHardLimit;
    }
    int bitsRequired = PackedInts.bitsRequired(numUniqueTerms) + 1;  // add one since we aren't using unsigned values for ords... (i.e. we would need to convert to unsigned or bias the values)
    // TODO: a good test that reliably fails if we didn't add 1 here!  CursorPagingTest is the only one that does fail.

    docToOrd = LongArray.create(maxDoc, bitsRequired);
    int termOrd = 0;


    final TermsEnum termsEnum = terms.iterator(null);
    DocsEnum docs = null;

    while(true) {
      final BytesRef term = termsEnum.next();
      if (term == null) {
        break;
      }
      if (termOrd >= termCountHardLimit) {
        break;
      }

      termOrd++;

      bytes.copyUsingLengthPrefix(term);
      docs = termsEnum.docs(null, docs, DocsEnum.FLAG_NONE);
      while (true) {
        final int docID = docs.nextDoc();
        if (docID == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        docToOrd.setLong(docID, termOrd);
      }
    }

    stats.numUniqueValues = termOrd;

    long termBytesLength = bytes.getUsedSize();
    termBytes = bytes.buildSingleArray();
    assert termBytesLength == HS.arraySizeBytes(termBytes);
    bytes.close();  // close early before building offset array to lower memory requirements


    MonotonicLongArray.Tracker tracker = new MonotonicLongArray.Tracker(termOrd, termBytesLength);
    long pos = 0;
    for (int i=0; i<termOrd; i++) {
      assert pos < termBytesLength;
      tracker.add(i, pos);
      int len = NativePagedBytes.getEntrySize(termBytes, pos);
      pos += len;
    }

    LongArray offsets = tracker.createArray();

    pos = 0;
    for (int i=0; i<termOrd; i++) {
      assert pos < termBytesLength;
      offsets.setLong(i, pos);
      int len = NativePagedBytes.getEntrySize(termBytes, pos);
      pos += len;
    }

    return new StrArrLeafValues(topValues.fieldValues, docToOrd, offsets, termBytes, stats);
  }


  /***
  public static class OrdIndexBuilder {
    StrFieldStats stats = new StrFieldStats();
    NativePagedBytes termBytesPaged = new NativePagedBytes(15);
    long termOrd = 0;

    // Results of the build.
    public LongArray docToOrd;
    public long termBytesArr;
    public LongArray offsets;

    public void addTerm(BytesRef term) throws IOException {
      termOrd++;
      termBytesPaged.copyUsingLengthPrefix(term);
    }

    public void addDoc(int doc) throws IOException {
      docToOrd.setLong(doc, termOrd);
    }

    public long getNumOrds() {
      return termOrd;
    }

    public void build() throws IOException {
      stats.numUniqueValues = termOrd;

      long termBytesLength = termBytesPaged.getUsedSize();
      termBytesArr = termBytesPaged.buildSingleArray();
      assert termBytesLength == HS.arraySizeBytes(termBytesArr);
      termBytesPaged.close();  // close early before building offset array to lower memory requirements
      termBytesPaged = null;

      MonotonicLongArray.Tracker tracker = new MonotonicLongArray.Tracker(termOrd, termBytesLength);
      long pos = 0;
      for (int i=0; i<termOrd; i++) {
        assert pos < termBytesLength;
        tracker.add(i, pos);
        int len = NativePagedBytes.getEntrySize(termBytesArr, pos);
        assert len > 0;
        pos += len;
      }

      LongArray offsets = tracker.createArray();

      pos = 0;
      for (int i=0; i<termOrd; i++) {
        assert pos < termBytesLength;
        offsets.setLong(i, pos);
        int len = NativePagedBytes.getEntrySize(termBytesArr, pos);
        pos += len;
      }
    }

    public StrLeafValues buildValues(FieldValues fieldValues) throws IOException {
      build();
      return new StrArrLeafValues(fieldValues, docToOrd, offsets, termBytesArr, stats);
    }
  }
  ***/

  @Override
  public StrTopValues create(SolrIndexSearcher.WarmContext warmContext) {
    StrTopValues tv = new StrTopValues((StrFieldValues)fieldValues);
    tv.create(warmContext, this);
    return tv;
  }
}

