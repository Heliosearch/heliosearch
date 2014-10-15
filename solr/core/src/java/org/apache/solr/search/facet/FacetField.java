package org.apache.solr.search.facet;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSetNative;
import org.apache.solr.search.field.FieldUtil;
import org.apache.solr.search.mutable.MutableValueInt;

public class FacetField extends FacetRequest {
  String field;
  long offset;
  long limit;
  long mincount;
  boolean missing;
  String prefix;
  String sortVariable;
  SortDirection sortDirection;
  FacetMethod method;
  boolean allBuckets;   // show cumulative stats across all buckets (this can be different than non-bucketed stats across all docs because of multi-valued docs)
  int cacheDf;  // 0 means "default", -1 means "never cache"

  // TODO: put this somewhere more generic?
  public static enum SortDirection {
    asc(-1) ,
    desc(1);

    private final int multiplier;
    private SortDirection(int multiplier) {
      this.multiplier = multiplier;
    }

    // asc==-1, desc==1
    public int getMultiplier() {
      return multiplier;
    }
  }

  public static enum FacetMethod {
    ENUM,
    STREAM,
    FIELDCACHE,
    SMART,
    ;

    public static FacetMethod fromString(String method) {
      if (method == null || method.length()==0) return null;
      if ("enum".equals(method)) {
        return ENUM;
      } else if ("fc".equals(method) || "fieldcache".equals(method)) {
        return FIELDCACHE;
      } else if ("smart".equals(method)) {
        return SMART;
      } else if ("stream".equals(method)) {
        return STREAM;
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown FacetField method " + method);
    }
  }


  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    SchemaField sf = fcontext.searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    boolean multiToken = sf.multiValued() || ft.multiValuedFieldCache();

    if (method == FacetMethod.ENUM && sf.indexed()) {
      throw new UnsupportedOperationException();
    } else if (method == FacetMethod.STREAM && sf.indexed()) {
      return new FacetFieldProcessorStream(fcontext, this, sf);
    }

    if (multiToken) {
      return new FacetFieldProcessorUIF(fcontext, this, sf);
    } else {
      // single valued string
      return new FacetFieldProcessorFC(fcontext, this, sf);
    }
  }
}



abstract class FacetFieldProcessor extends FacetProcessor<FacetField> {
  SchemaField sf;
  SlotAcc sortAcc;

  FacetFieldProcessor(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq);
    this.sf = sf;
  }

  @Override
  public Object getResponse() {
    return response;
  }

  void setSortAcc(int numSlots) {
    String sortKey = freq.sortVariable;
    sortAcc = accMap.get(sortKey);

    if (sortAcc == null) {
      if ("count".equals(sortKey)) {
        sortAcc = countAcc;
      } else if ("index".equals(sortKey)) {
        sortAcc = new SortSlotAcc(slot);
        // This sorting accumulator just goes by the slot number, so does not need to be collected
        // and hence does not need to find it's way into the accMap or accs array.
      }
    }
  }

  static class Slot {
    int slot;
  }
}


// base class for FC style of facet counting (single and multi-valued strings)
abstract class FacetFieldProcessorFCBase extends FacetFieldProcessor {
  BytesRef prefixRef;
  int startTermIndex;
  int endTermIndex;
  int nTerms;
  int nDocs;


  public FacetFieldProcessorFCBase(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  public void process() throws IOException {
    sf = fcontext.searcher.getSchema().getField(freq.field);
    response = getFieldCacheCounts();
  }


  abstract protected BytesRef lookupOrd(int ord) throws IOException;
  abstract protected void findStartAndEndOrds() throws IOException;
  abstract protected void collectDocs() throws IOException;


  public SimpleOrderedMap<Object> getFieldCacheCounts() throws IOException {
    String prefix = freq.prefix;
    if (prefix == null || prefix.length() == 0) {
      prefixRef = null;
    } else {
      prefixRef = new BytesRef(prefix);
    }

    findStartAndEndOrds();

    createAccs(nDocs, nTerms);
    setSortAcc(nTerms);
    prepareForCollection();

    collectDocs();

    return findTopSlots();
  }


  protected SimpleOrderedMap<Object> findTopSlots() throws IOException {
    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

    int off = (int) freq.offset;
    int lim = freq.limit >= 0 ? (int) freq.limit : Integer.MAX_VALUE;

    int maxsize = freq.limit > 0 ? (int) freq.offset + (int) freq.limit : Integer.MAX_VALUE - 1;
    maxsize = Math.min(maxsize, nTerms);

    final int sortMul = freq.sortDirection.getMultiplier();
    final SlotAcc sortAcc = this.sortAcc;

    PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxsize) {
      @Override
      protected boolean lessThan(Slot a, Slot b) {
        int cmp = sortAcc.compare(a.slot, b.slot) * sortMul;
        return cmp == 0 ? b.slot < a.slot : cmp < 0;
      }
    };

    Slot bottom = null;
    for (int i = (startTermIndex == -1) ? 1 : 0; i < nTerms; i++) {
      if (freq.mincount > 0 && countAcc.getCount(i) < freq.mincount) {
        continue;
      }

      if (bottom != null) {
        if (sortAcc.compare(bottom.slot, i) * sortMul < 0) {
          bottom.slot = i;
          bottom = queue.updateTop();
        }
      } else {
        // queue not full
        Slot s = new Slot();
        s.slot = i;
        queue.add(s);
        if (queue.size() >= maxsize) {
          bottom = queue.top();
        }
      }
    }


    // if we are deep paging, we don't have to order the highest "offset" counts.
    int collectCount = Math.max(0, queue.size() - off);
    assert collectCount <= lim;
    int[] sortedSlots = new int[collectCount];
    for (int i = collectCount - 1; i >= 0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    if (freq.allBuckets) {
      SimpleOrderedMap<Object> allBuckets = new SimpleOrderedMap<>();
      for (SlotAcc acc : accs) {
        countAcc.setValues(allBuckets, -1);
        acc.setValues(allBuckets, -1);
      }
      res.add("allBuckets", allBuckets);
    }

    ArrayList bucketList = new ArrayList(collectCount);
    res.add("buckets", bucketList);


    for (int slotNum : sortedSlots) {
      SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

      // get the ord of the slot...
      int ord = startTermIndex + slotNum;

      BytesRef br;
      Object val;
      if (startTermIndex == -1 && slotNum == 0) {
        // this is the "missing" bucket
        val = null;
        br = new BytesRef();
      } else {
        br = lookupOrd(ord);
        val = sf.getType().toObject(sf, br);
      }

      bucket.add("val", val);
      // add stats for this bucket
      addStats(bucket, slotNum);

      // handle sub-facets for this bucket
      if (freq.getSubFacets().size() > 0) {
        FacetContext subContext = fcontext.sub();
        subContext.base = fcontext.searcher.getDocSet(new TermQuery(new Term(sf.getName(), br.clone())));
        try {
          fillBucketSubs(bucket, subContext);
        } finally {
          subContext.base.decref();
          // subContext.base = null;  // do not modify context after creation... there may be deferred execution (i.e. streaming)
        }
      }

      bucketList.add(bucket);
    }

    return res;
  }


}


class FacetFieldProcessorFC extends FacetFieldProcessorFCBase {
  SortedDocValues sortedDocValues;


  public FacetFieldProcessorFC(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  protected BytesRef lookupOrd(int ord) throws IOException {
    return sortedDocValues.lookupOrd(ord);
  }

  protected void findStartAndEndOrds() throws IOException {
    sortedDocValues = FieldUtil.getSortedDocValues(fcontext.qcontext, sf, null);

    if (prefixRef != null) {
      startTermIndex = sortedDocValues.lookupTerm(prefixRef);
      if (startTermIndex < 0) startTermIndex = -startTermIndex - 1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = sortedDocValues.lookupTerm(prefixRef);
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex - 1;
    } else {
      startTermIndex = freq.missing ? -1 : 0;
      endTermIndex = sortedDocValues.getValueCount();
    }

    nTerms = endTermIndex - startTermIndex;
  }

  protected void collectDocs() throws IOException {
    final MutableValueInt slot = this.slot;

    // TODO: do stats include "missing"???

    // count collection array only needs to be as big as the number of terms we are
    // going to collect counts for.
    // final int[] counts = new int[nTerms]; // TODO

    final List<AtomicReaderContext> leaves = fcontext.searcher.getIndexReader().leaves();
    final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
    AtomicReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = fcontext.base.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (doc >= adjustedMax) {
        do {
          ctx = ctxIt.next();
          if (ctx == null) {
            // should be impossible
            throw new RuntimeException("INTERNAL FACET ERROR");
          }
          segBase = ctx.docBase;
          segMax = ctx.reader().maxDoc();
          adjustedMax = segBase + segMax;
        } while (doc >= adjustedMax);
        assert doc >= ctx.docBase;
        setNextReader(ctx);
      }

      int term = sortedDocValues.getOrd( doc );
      int arrIdx = term - startTermIndex;
      if (arrIdx>=0 && arrIdx<nTerms) {
        slot.value = arrIdx;
        countAcc.incrementCount(arrIdx, 1);
        collect(doc - segBase);  // per-seg collectors
        // counts[arrIdx]++;
      }
    }
  }

}

// UnInvertedField implementation of field faceting
class FacetFieldProcessorUIF extends FacetFieldProcessorFC {
  UnInvertedField uif;
  TermsEnum te;

  FacetFieldProcessorUIF(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  protected void findStartAndEndOrds() throws IOException {
    uif = UnInvertedField.getUnInvertedField(freq.field, fcontext.searcher);
    te = uif.getOrdTermsEnum( fcontext.searcher.getAtomicReader() );

    startTermIndex = 0;
    endTermIndex = uif.numTerms();  // one past the end

    if (prefixRef != null) {
      if (te.seekCeil(prefixRef) == TermsEnum.SeekStatus.END) {
        startTermIndex = uif.numTerms();
      } else {
        startTermIndex = (int) te.ord();
      }
      prefixRef.append(UnicodeUtil.BIG_TERM);
      if (te.seekCeil(prefixRef) == TermsEnum.SeekStatus.END) {
        endTermIndex = uif.numTerms();
      } else {
        endTermIndex = (int) te.ord();
      }
    }

    nTerms = endTermIndex - startTermIndex;
  }

  @Override
  protected BytesRef lookupOrd(int ord) throws IOException {
    return uif.getTermValue(te, ord);
  }

  @Override
  protected void collectDocs() throws IOException {
    uif.collectDocs(this);
  }
}



class FacetFieldProcessorStream extends FacetFieldProcessor implements Closeable {
  long bucketsToSkip;
  long bucketsReturned;

  boolean closed;
  boolean countOnly;
  boolean hasSubFacets;  // true if there are subfacets
  int minDfFilterCache;
  DocSet docs;
  DocSet fastForRandomSet;
  TermsEnum termsEnum = null;
  SolrIndexSearcher.DocsEnumState deState = null;
  DocsEnum docsEnum;
  BytesRef startTermBytes;
  BytesRef term;
  AtomicReaderContext[] leaves;



  FacetFieldProcessorStream(FacetContext fcontext, FacetField freq, SchemaField sf) {
    super(fcontext, freq, sf);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      fcontext.base.decref();
    }
  }


  @Override
  public void process() throws IOException {
    // We need to keep the fcontext open after processing is done (since we will be streaming in the response writer).
    // But if the connection is broken, we want to clean up.
    fcontext.base.incref();
    fcontext.qcontext.addCloseHook(this);

    setup();
    response = new SimpleOrderedMap<>();
    response.add( "buckets", new Iterator() {
      boolean retrieveNext = true;
      Object val;
      @Override
      public boolean hasNext() {
        if (retrieveNext) {
          val = nextBucket();
        }
        retrieveNext = false;
        return val != null;
      }

      @Override
      public Object next() {
        if (retrieveNext) {
          val = nextBucket();
        }
        retrieveNext = true;
        if (val == null) {
          // Last value, so clean up.  In the case that we are doing streaming facets within streaming facets,
          // the number of close hooks could grow very large, so we want to remove ourselves.
          boolean removed = fcontext.qcontext.removeCloseHook(FacetFieldProcessorStream.this);
          assert removed;
          try {
            close();
          } catch (IOException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error during facet streaming close", e);
          }
        }
        return val;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    });
  }



  public void setup() throws IOException {

    countOnly = freq.facetStats.size() == 0 || freq.facetStats.values().iterator().next() instanceof CountAgg;
    hasSubFacets = freq.subFacets.size() > 0;
    bucketsToSkip = freq.offset;



    createAccs(-1, 1);
    prepareForCollection();

    // Minimum term docFreq in order to use the filterCache for that term.
    int defaultMinDf = Math.max(fcontext.searcher.maxDoc() >> 4, 3);  // (minimum of 3 is for test coverage purposes)
    int minDfFilterCache = freq.cacheDf == 0 ? defaultMinDf : freq.cacheDf;
    if (minDfFilterCache == -1) minDfFilterCache = Integer.MAX_VALUE;  // -1 means never cache

    docs = fcontext.base;
    fastForRandomSet = null;

    if (freq.prefix != null) {
      String indexedPrefix = sf.getType().toInternal(freq.prefix);
      startTermBytes = new BytesRef(indexedPrefix);
    }

    Fields fields = fcontext.searcher.getAtomicReader().fields();
    Terms terms = fields == null ? null : fields.terms(sf.getName());


    termsEnum = null;
    deState = null;
    term = null;


    if (terms != null) {

      termsEnum = terms.iterator(null);

      // TODO: OPT: if seek(ord) is supported for this termsEnum, then we could use it for
      // facet.offset when sorting by index order.

      if (startTermBytes != null) {
        if (termsEnum.seekCeil(startTermBytes) == TermsEnum.SeekStatus.END) {
          termsEnum = null;
        } else {
          term = termsEnum.term();
        }
      } else {
        // position termsEnum on first term
        term = termsEnum.next();
      }
    }

    List<AtomicReaderContext> leafList = fcontext.searcher.getTopReaderContext().leaves();
    leaves = leafList.toArray( new AtomicReaderContext[ leafList.size() ]);


  }


  public SimpleOrderedMap<Object> nextBucket() {
    try {
      return _nextBucket();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error during facet streaming", e);
    }
  }

  public SimpleOrderedMap<Object> _nextBucket() throws IOException {
    int mincount = (int)freq.mincount;
    DocSet termSet = null;

    try {
      while (term != null) {

        if (startTermBytes != null && !StringHelper.startsWith(term, startTermBytes)) {
          break;
        }

        int df = termsEnum.docFreq();
        if (df < mincount) {
          term = termsEnum.next();
          continue;
        }

        if (termSet != null) {
          termSet.decref();
          termSet = null;
        }

        int c = 0;

        if (hasSubFacets || df >= minDfFilterCache) {
          // use the filter cache

          if (deState == null) {
            deState = new SolrIndexSearcher.DocsEnumState();
            deState.fieldName = sf.getName();
            deState.liveDocs = fcontext.searcher.getAtomicReader().getLiveDocs();
            deState.termsEnum = termsEnum;
            deState.docsEnum = docsEnum;
            deState.minSetSizeCached = minDfFilterCache;
          }

            if (hasSubFacets || !countOnly) {
              DocSet termsAll = fcontext.searcher.getDocSet(deState);
              termSet = docs.intersection(termsAll);
              termsAll.decref();
              c = termSet.size();
            } else {
              c = fcontext.searcher.numDocs(docs, deState);
            }
            docsEnum = deState.docsEnum;

            resetStats();

            if (!countOnly) {
              collect(termSet);
            }

        } else {
          // We don't need the docset here (meaning no sub-facets).
          // if countOnly, then we are calculating some other stats...
          resetStats();

          // lazy convert to fastForRandomSet
          if (fastForRandomSet == null) {
            fastForRandomSet = docs;
            if (docs instanceof SortedIntDocSetNative) {
              SortedIntDocSetNative sset = (SortedIntDocSetNative) docs;
              fastForRandomSet = new HashDocSet(sset.getIntArrayPointer(), 0, sset.size(), HashDocSet.DEFAULT_INVERSE_LOAD_FACTOR);
            }
          }
          // iterate over TermDocs to calculate the intersection

          docsEnum = termsEnum.docs(null, docsEnum, DocsEnum.FLAG_NONE);

          if (docsEnum instanceof MultiDocsEnum) {
            MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum) docsEnum).getSubs();
            int numSubs = ((MultiDocsEnum) docsEnum).getNumSubs();
            for (int subindex = 0; subindex < numSubs; subindex++) {
              MultiDocsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.docsEnum == null) continue;
              int base = sub.slice.start;
              int docid;

              if (countOnly) {
                while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid + base)) c++;
                }
              } else {
                setNextReader(leaves[sub.slice.readerIndex]);
                while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (fastForRandomSet.exists(docid + base)) {
                    c++;
                    collect(docid);
                  }
                }
              }

            }
          } else {
            int docid;
            if (countOnly) {
              while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) c++;
              }
            } else {
              setNextReader(leaves[0]);
              while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid)) {
                  c++;
                  collect(docid);
                }
              }
            }
          }

        }



        if (c < mincount) {
          term = termsEnum.next();
          continue;
        }

        // handle offset and limit
        if (bucketsToSkip > 0) {
          bucketsToSkip--;
          term = termsEnum.next();
          continue;
        }

        if (freq.limit >= 0 && ++bucketsReturned > freq.limit) {
          return null;
        }

        // set count in case other stats depend on it
        countAcc.incrementCount(0, c);

        // OK, we have a good bucket to return... first get bucket value before moving to next term
        Object bucketVal = sf.getType().toObject(sf, term);
        term = termsEnum.next();

        SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();
        bucket.add("val", bucketVal);
        addStats(bucket, 0);
        if (hasSubFacets) {
          processSubs(bucket, termSet);
        }

        // TODO... termSet needs to stick around for streaming sub-facets?

        return bucket;

      }

    } finally {
      if (termSet != null) {
        termSet.decref();
        termSet = null;
      }
    }


    // end of the iteration
    return null;
  }



}