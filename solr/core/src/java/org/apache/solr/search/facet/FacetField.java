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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
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
    FIELDCACHE
    ;

    public static FacetMethod fromString(String method) {
      if (method == null || method.length()==0) return null;
      if ("enum".equals(method)) {
        return ENUM;
      } else if ("fc".equals(method) || "fieldcache".equals(method)) {
        return FIELDCACHE;
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown FacetField method " + method);
    }
  }


  @Override
  public FacetProcessor createFacetProcessor(FacetContext fcontext) {
    SchemaField sf = fcontext.searcher.getSchema().getField(field);
    FieldType ft = sf.getType();
    boolean multiToken = sf.multiValued() || sf.getType().multiValuedFieldCache();

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
        if (countAcc == null) {
          countAcc = new CountSlotAcc(slot, fcontext.qcontext, numSlots);
          countAcc.key = "count";  // if we want it to appear
          accMap.put("count", countAcc);
        }
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
      if (countAcc != null && freq.mincount > 0) {
        // TODO: track counts separately?
        int slotDocCount = ((Number) countAcc.getValue(i)).intValue();
        if (slotDocCount < freq.mincount) {
          continue;
        }
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
        acc.setValues(allBuckets, -1);
      }
      res.add("allBuckets", allBuckets);
    }

    ArrayList bucketList = new ArrayList(collectCount);
    res.add("buckets", bucketList);

    FacetContext subContext = null;
    if (freq.getSubFacets().size() > 0) {
      subContext = fcontext.sub();
    }

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
      if (subContext != null) {
        subContext.base = fcontext.searcher.getDocSet(new TermQuery(new Term(sf.getName(), br.clone())));
        try {
          fillBucketSubs(bucket, subContext);
        } finally {
          subContext.base.decref();
          subContext.base = null;
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
