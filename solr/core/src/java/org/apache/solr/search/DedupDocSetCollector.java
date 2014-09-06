package org.apache.solr.search;

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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.solr.core.HS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DedupDocSetCollector must be closed after use.
 */

public class DedupDocSetCollector extends Collector implements AutoCloseable {
  private long buffer;
  private BitDocSetNative bits;
  private int globalPos = 0;
  private int pos=0;
  private final int maxDoc;
  private final int smallSetSize;
  private final int collectLimit;
  private int base;
  private final int bufferSize = HS.BUFFER_SIZE_BYTES >>> 2;
  private List<Long> bufferList;

  public DedupDocSetCollector(int smallSetSize, int maxDoc) {
    this.smallSetSize = smallSetSize;
    this.collectLimit = Math.min((smallSetSize>>1) + 5, smallSetSize);
    this.maxDoc = maxDoc;
    allocBuffer();
  }

  private void allocBuffer() {
    buffer = HS.getBuffer();
  }

  @Override
  public void collect(int doc) throws IOException {
    if (pos >= bufferSize) {
      newBuffer();
    }

    doc += base;
    HS.setInt(buffer, pos, doc);
    pos++;
  }

  private int bufferedSize() {
    return globalPos + pos;
  }

  private void newBuffer() {
    assert pos == bufferSize;
    globalPos += pos;
    pos = 0;  // do this here so bufferedSize will work

    if (bits == null && bufferedSize() > collectLimit) {
      bits = new BitDocSetNative(maxDoc);
    }

    // if we've already transitioned to a bitset, then just set the bits
    // and reuse this buffer.
    if (bits != null) {
      setBits(buffer, bufferSize);
      return;
    }

    if (bufferList == null) {
      bufferList = new ArrayList<>();
    }

    bufferList.add(buffer);
    buffer = 0;  // zero out in case allocBuffer fails
    allocBuffer();
  }

  private void setBits(long buf, int sz) {
    BitDocSetNative.setBits(bits.array, bits.wlen, buf, sz);
    /**
    for (int i=0; i<sz; i++) {
      bits.fastSet( HS.getInt(buf, i) );
    }
    **/
  }

  private static DocSet makeSmallSet(BitDocSetNative bits) throws IOException {
    int numDocs = (int)bits.cardinality();
    long answer = HS.allocArray(numDocs, 4, false);
    DocIdSetIterator iter = bits.docIterator();
    for(int i=0; i<numDocs; i++) {
      int docid = iter.nextDoc();
      HS.setInt(answer, i, docid);
    }
    assert iter.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
    return new SortedIntDocSetNative(answer, numDocs);
  }


  public DocSet getDocSet() throws IOException {
    int sz = bufferedSize();

    if (bits == null && sz > collectLimit) {
      bits = new BitDocSetNative(maxDoc);
    }

    if (bits != null) {
      setBits(buffer, pos);

      if (bufferList != null) {
        for (long buf : bufferList) {
          setBits(buf, bufferSize);
        }
      }

      if (sz > smallSetSize) {
        DocSet answer = bits;
        bits = null; // null out so we know we don't need to free later
        return answer;
      } else {
        return makeSmallSet(bits);
      }
    }

    // make a small set
    long all;
    if (bufferList == null) {
      all = buffer;  // steal the buffer
      buffer = 0;
    } else {
      all = HS.allocArray(sz, 4, false);
      int allPos = 0;
      for (long buf : bufferList) {
        HS.copyInts(buf, 0, all, allPos, bufferSize);
        allPos += bufferSize;
      }
      HS.copyInts(buffer, 0, all, allPos, pos);
      allPos += pos;
      assert allPos == sz;
    }

    int nDocs = HS.sortDedupInts(all, sz, maxDoc);

    // resize if more than 1/16 slop after dedup, or if we are using a buffer pool buffer
    if (bufferList==null || nDocs < sz - (sz>>4)) {
      long arr2 = HS.allocArray(nDocs, 4, false);
      HS.copyInts(all, 0, arr2, 0, nDocs);
      HS.freeArray(all);
      all = arr2;
    }

    return new SortedIntDocSetNative(all, nDocs);
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.base = context.docBase;
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void close() {
    if (bits != null) {
      bits.decref();
      bits = null;
    }
    if (buffer != 0) {
      HS.freeArray(buffer);
      buffer = 0;
    }
    if (bufferList != null) {
      for (long buf : bufferList) {
        HS.freeArray(buf);
      }
      bufferList = null;
    }
  }
}
