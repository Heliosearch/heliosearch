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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.RefCount;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/** A base class that may be useful for implementing DocSets */
public abstract class DocSetBaseNative implements RefCount, DocSet {
  public static Logger log = SolrCore.log;

  private final AtomicInteger refcount = new AtomicInteger(1);

  public static void debug(boolean clear) {}

  /********************* uncomment to track all incref/decref calls
  public static Map<DocSetBaseNative, Object> debugMap = new IdentityHashMap<>();

  public static void debug(boolean clear) {
    Collection<DocSetBaseNative> sets;

    synchronized (DocSetBaseNative.class) {
      if (debugMap.isEmpty()) return;
      sets = new ArrayList<>(debugMap.keySet());
      if (clear) debugMap.clear();  // avoid all calls after this point from seeing same leaks
    }

    log.error("DOCSET ALLOCATION LIST size=" + sets.size());
    int show=20;
    for (DocSetBaseNative set : sets) {
      if (--show < 0) break;
      log.error(" ###### SET " + set + " refcount=" + set.refcount.get() + " events=" + set.events);
    }

    // throw new RuntimeException("LEAKED DOCSETS");
  }

  private static class Trace {
    String event;
    StackTraceElement[] trace;
    public String toString() {
      return event + Diagnostics.toString(trace, DocSetBaseNative.class.getSimpleName());
    }
  }

  private List<Trace> events = new ArrayList<Trace>();

  private Trace whereAmI(String event) {
    Trace debug = new Trace();
    debug.event = event;
    debug.trace = Thread.currentThread().getStackTrace();
    return debug;
  }

  {
    events.add( whereAmI("ALLOC ") );
    synchronized (DocSetBaseNative.class) {
      debugMap.put(this, this);
    }
  }

  private void debug_incref() {
    synchronized (events) {
      events.add( whereAmI("INCREF cnt=" + (refcount.get()+1) + " :") );
    }

    if (refcount.get() <= 0) {
      SolrCore.log.error("TRYING TO INCREF DEAD DOCSET : " + this + "\n" + events);
      throw new RuntimeException("TRYING TO INCREF DEAD DOCSET");
    }
  }

  private void debug_decref() {
    synchronized (events) {
      events.add( whereAmI("DECREF cnt=" + (refcount.get()-1) + " :") );
    }

    if (refcount.get() <= 0) {
      SolrCore.log.error("TRYING TO FREE DEAD DOCSET :" + this + " :" + events);
      throw new RuntimeException("TRYING TO FREE DEAD DOCSET");
    }

    if (refcount.get() == 1) {
      debugMap.remove(this);
    }
  }
  ******************************************************/


  @Override
  public int getRefCount() {
    return refcount.get();
  }

  @Override
  public int incref() {
    // debug_incref();

    int count;
    while ((count = refcount.get()) > 0) {
      if (refcount.compareAndSet(count, count+1)) {
        return count+1;
      }
    }
    throw new RuntimeException("Trying to incref freed native DocSet " + this);
  }

  @Override
  public int decref() {
    // debug_decref();

    int count;
    while ((count = refcount.get()) > 0) {
      int newCount = count - 1;
      if (refcount.compareAndSet(count, newCount)) {
        if (newCount == 0) {
          free();
        }
        return newCount;
      }
    }

    throw new RuntimeException("Too many decrefs detected for native DocSet " + this);
  }


  @Override
  public boolean tryIncref() {
    // debug_incref();

    int count;
    while ((count = refcount.get()) > 0) {
      if (refcount.compareAndSet(count, count+1)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean tryDecref() {
    // debug_decref();

    int count;
    while ((count = refcount.get()) > 0) {
      int newCount = count - 1;
      if (refcount.compareAndSet(count, newCount)) {
        if (newCount == 0) {
          free();
        }
        return true;
      }
    }

    return false;
  }


  protected abstract void free();

  @Override  // for AutoCloseable
  public void close() {
    decref();
  }

  // Not implemented efficiently... for testing purposes only
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DocSet)) return false;
    DocSet other = (DocSet)obj;
    if (this.size() != other.size()) return false;

    if (this instanceof DocList && other instanceof DocList) {
      // compare ordering
      DocIterator i1=this.iterator();
      DocIterator i2=other.iterator();
      while(i1.hasNext() && i2.hasNext()) {
        if (i1.nextDoc() != i2.nextDoc()) return false;
      }
      return true;
      // don't compare matches
    }

    // if (this.size() != other.size()) return false;
    return this.getBits().equals(other.getBits());
  }

  /**
   * @throws org.apache.solr.common.SolrException Base implementation does not allow modifications
   */
  @Override
  public void add(int doc) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Unsupported Operation");
  }

  /**
   * @throws org.apache.solr.common.SolrException Base implementation does not allow modifications
   */
  @Override
  public void addUnique(int doc) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Unsupported Operation");
  }

  /**
   * Inefficient base implementation.
   */
  @Override
  public FixedBitSet getBits() {
    FixedBitSet bits = new FixedBitSet(64);
    for (DocIterator iter = iterator(); iter.hasNext();) {
      int nextDoc = iter.nextDoc();
      bits = FixedBitSet.ensureCapacity(bits, nextDoc);
      bits.set(nextDoc);
    }
    return bits;
  }

  @Override
  public DocSet intersection(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSetNative)) {
      return other.intersection(this);
    }

    // Default... handle with bitsets.
    FixedBitSet newbits = (FixedBitSet)(this.getBits().clone());
    newbits.and(other.getBits());
    return new BitDocSetNative(newbits);
  }

  @Override
  public boolean intersects(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSetNative)) {
      return other.intersects(this);
    }
    // less efficient way: get the intersection size
    return intersectionSize(other) > 0;
  }


  @Override
  public DocSet union(DocSet other) {
    if (other instanceof BitDocSetNative) {
      return other.union(this);
    }
    FixedBitSet newbits = (FixedBitSet)(this.getBits().clone());
    FixedBitSet otherbits = other.getBits();
    newbits = FixedBitSet.ensureCapacity(newbits, otherbits.length());
    newbits.or(otherbits);
    return new BitDocSetNative(newbits);
  }

  @Override
  public int intersectionSize(DocSet other) {
    // intersection is overloaded in the smaller DocSets to be more
    // efficient, so dispatch off of it instead.
    if (!(other instanceof BitDocSetNative)) {
      return other.intersectionSize(this);
    }
    // less efficient way: do the intersection then get it's size
    return intersection(other).size();
  }

  @Override
  public int unionSize(DocSet other) {
    return this.size() + other.size() - this.intersectionSize(other);
  }

  @Override
  public DocSet andNot(DocSet other) {
    FixedBitSet newbits = (FixedBitSet)(this.getBits().clone());
    newbits.andNot(other.getBits());
    return new BitDocSetNative(newbits);
  }

  @Override
  public int andNotSize(DocSet other) {
    return this.size() - this.intersectionSize(other);
  }

  @Override
  public Filter getTopFilter() {
    final FixedBitSet bs = getBits();

    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, Bits acceptDocs) {
        AtomicReader reader = context.reader();
        // all Solr DocSets that are used as filters only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        if (context.isTopLevel) {
          return BitsFilteredDocIdSet.wrap(bs, acceptDocs);
        }

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.

        return BitsFilteredDocIdSet.wrap(new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int pos=base-1;
              int adjustedDoc=-1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() {
                pos = bs.nextSetBit(pos+1);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public int advance(int target) {
                if (target==NO_MORE_DOCS) return adjustedDoc=NO_MORE_DOCS;
                pos = bs.nextSetBit(target+base);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public long cost() {
                return bs.length();
              }
            };
          }

          @Override
          public boolean isCacheable() {
            return true;
          }

          @Override
          public Bits bits() {
            // sparse filters should not use random access
            return null;
          }

        }, acceptDocs2);
      }
    };
  }

  @Override
  public void setBitsOn(FixedBitSet target) {
    DocIterator iter = iterator();
    while (iter.hasNext()) {
      target.set(iter.nextDoc());
    }
  }

  @Override
  public void setBitsOn(BitDocSetNative target) {
    DocIterator iter = iterator();
    while (iter.hasNext()) {
      target.fastSet(iter.nextDoc());
    }
  }

  @Override
  public void addAllTo(DocSet target) {
    DocIterator iter = iterator();
    while (iter.hasNext()) {
      target.add(iter.nextDoc());
    }
  }

  @Override
  public abstract DocSet clone();

}
