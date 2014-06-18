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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.field.FieldUtil;
import org.apache.solr.search.mutable.MutableValueInt;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SimpleFacetStats implements Closeable {
  SimpleFacets simpleFacets;

  public SimpleFacetStats(SimpleFacets simpleFacets) {
    this.simpleFacets = simpleFacets;
  }

  // set by creator
  LinkedHashMap<String,AccInfo> accInfos = new LinkedHashMap<>();
  String sortStr;
  private List<SimpleFacets.Subfacet> subFacets;



  LinkedHashMap<String,SlotAcc> accs = new LinkedHashMap<>();
  SlotAcc[] accArr;
  SlotAcc sortAcc;
  SlotAcc countAcc;
  int sortMul;
  MutableValueInt slot = new MutableValueInt();
  QueryContext queryContext;



  public void setStat(String stat) {
    AccInfo info = AccInfo.parseFacetStat(stat, simpleFacets.req);
    accInfos.put(info.key, info);
  }

  public void setStats(String[] stats) {
    if (stats == null) return;

    for (String stat : stats) {
      setStat(stat);
    }
  }

  public void setSort(String facetSort) {
    if (facetSort == null) {
      facetSort="count";
    }

    sortStr = facetSort;
  }

  public void setSubFacets(List<SimpleFacets.Subfacet> subFacets) {
    this.subFacets = subFacets;
  }

  private void clear() throws IOException {
    if (accArr != null) {
      for (Acc acc: accArr) {
        acc.close();
      }
      accArr = null;
      sortAcc = null;
      countAcc = null;
    }
  }

  @Override
  public void close() throws IOException {
    clear();
  }


  void createAccs(int numDocs, int numSlots) throws IOException {
    // count can be used in a couple of places...
    // average can be derived from sum and count

    clear();

    queryContext = QueryContext.newContext(simpleFacets.searcher);
    for (AccInfo info : accInfos.values()) {
      SlotAcc acc = info.createSlotAcc(slot, queryContext, simpleFacets.req, simpleFacets.searcher, numDocs, numSlots);
      accs.put(info.key, acc);
      if (acc instanceof CountSlotAcc) {
        countAcc = acc;
      }
    }


    String sortKey = sortStr;  // TODO: asc/desc

    if (sortStr != null) {
      String stat =  sortStr;
      String direction = "desc";

      if (sortStr.endsWith(" asc")) {
        stat=sortStr.substring(0, sortStr.length()-" asc".length());
        direction = "asc";
      } else if (sortStr.endsWith(" desc")) {
        stat=sortStr.substring(0, sortStr.length()-" desc".length());
        direction = "desc";
      } else {
        direction = "index".equals(sortStr) ? "asc" : "desc";  // default direction for "index" is ascending
      }

      sortAcc = accs.get(stat);

      if (sortAcc == null) {
        if ("count".equals(stat)) {
          if (countAcc == null) {
            countAcc = new CountSlotAcc(slot, queryContext, numSlots);
            countAcc.key = "count";  // if we want it to appear
            accs.put("count", countAcc);
          }
          sortAcc = countAcc;
        } else if ("index".equals(stat)) {
          sortAcc = new SortSlotAcc(slot);
          if (sortStr.equals("index")) {
            direction = "asc";
          }
        }
      }

      sortMul = direction.equals("desc") ? 1 : -1;

      if (sortAcc == null || !(sortAcc instanceof SlotAcc)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "facet.stat - invalid sort specified: '" + sortKey + "'");
      }
    }

    accArr = accs.values().toArray(new SlotAcc[accs.size()]);
  }

  void setNextReader(AtomicReaderContext ctx) throws IOException {
    for (SlotAcc acc : accArr) {
      acc.setNextReader(ctx);
    }
  }

  void collect(int doc) throws IOException {
    for (SlotAcc acc : accArr) {
      acc.collect(doc);
    }
  }


  void addStats(NamedList<Object> target, int slotNum) {
    slot.value = slotNum;
    for (Map.Entry<String,SlotAcc> entry : accs.entrySet()) {
      // Comparable val = entry.getValue().getValue();
      // target.add(entry.getKey(), val);
      entry.getValue().setValues(target, slotNum);
    }
  }

  void addGlobalStats(SimpleOrderedMap<Object> target) {
    for (Map.Entry<String,SlotAcc> entry : accs.entrySet()) {
      // Comparable val = entry.getValue().getGlobalValue();
      // target.add(entry.getKey(), val);
      entry.getValue().setValues(target, -1);
    }
  }

  public int collect(int slotNum, DocSet docs) throws IOException {
    slot.value = slotNum;
    return collect(docs);
  }

  public void collect(int slotNum, int segDoc) throws IOException {
    slot.value = slotNum;
    collect(segDoc);
  }

  public int collect(DocSet docs) throws IOException {
    int count = 0;
    SolrIndexSearcher searcher = simpleFacets.searcher;

    final List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
    AtomicReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
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
      count++;
      collect(doc - segBase);  // per-seg collectors
    }
    return count;
  }


  public SimpleOrderedMap<Object> getQueryFacet(DocSet base, Query q) throws IOException {
    SolrIndexSearcher searcher = simpleFacets.searcher;
    DocSet docs = q instanceof MatchAllDocsQuery ? base : searcher.getDocSet(q, base);

    try {
      SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

      int sz = docs.size();

      createAccs(sz, 1);

      collect(0, docs);

      addStats(res, 0);

      addSubFacets(res, docs, q);

      return res;

    } finally {
      if (docs != base) {
        docs.decref();
      }
      clear();
    }
  }

  public SimpleOrderedMap<Object> getRangeBucket(DocSet base, Query rangeQ, Object label, SchemaField sf, String low, String high, boolean iLow, boolean iHigh) throws IOException {
    SimpleOrderedMap<Object> ret = new SimpleOrderedMap<>();

    // typically the start value of the range, but null for before/after/between
    if (label != null) {
      ret.add("val", label);
    }

    SimpleOrderedMap stats = getQueryFacet(base, rangeQ);
    ret.addAll(stats);
    return ret;
  }

  static class Slot {
    int slot;
  }

  public SimpleOrderedMap<Object> getFieldCacheCounts(DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String prefix) throws IOException {
    try {
      return _getFieldCacheCounts(docs, fieldName, offset, limit, mincount, missing, prefix);
    } finally {
      clear();
    }
  }

  public SimpleOrderedMap<Object> _getFieldCacheCounts(DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String prefix) throws IOException {
    SolrIndexSearcher searcher = simpleFacets.searcher;
    SchemaField sf = searcher.getSchema().getField(fieldName);
    FieldType ft = sf.getType();
    // SortedDocValues si = FieldCache.DEFAULT.getTermsIndex(searcher.getAtomicReader(), fieldName);
    QueryContext qcontext = QueryContext.newContext(searcher);
    SortedDocValues si = FieldUtil.getSortedDocValues(qcontext, sf, null);

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

    final BytesRef prefixRef;
    if (prefix == null) {
      prefixRef = null;
    } else if (prefix.length()==0) {
      prefix = null;
      prefixRef = null;
    } else {
      prefixRef = new BytesRef(prefix);
    }

    int startTermIndex, endTermIndex;
    if (prefix!=null) {
      startTermIndex = si.lookupTerm(prefixRef);
      if (startTermIndex<0) startTermIndex=-startTermIndex-1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = si.lookupTerm(prefixRef);
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex-1;
    } else {
      startTermIndex = missing ? -1 : 0;
      endTermIndex=si.getValueCount();
    }


    final int nTerms=endTermIndex-startTermIndex;
    int nDocs = docs.size();

    createAccs(nDocs, nTerms);

    final MutableValueInt slot = this.slot;

    // TODO: do stats include "missing"???
    // Perhaps that should depend on the value of the "missing" variable!!!



    int missingCount = -1;
    final CharsRef charsRef = new CharsRef(10);

    // count collection array only needs to be as big as the number of terms we are
    // going to collect counts for.
    final int[] counts = new int[nTerms];

    DocIterator iter = docs.iterator();


    final List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
    AtomicReaderContext ctx = null;
    int segBase = 0;
    int segMax;
    int adjustedMax = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
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

      int term = si.getOrd(iter.nextDoc());
      int arrIdx = term-startTermIndex;
      if (arrIdx>=0 && arrIdx<nTerms) {
        slot.value = arrIdx;
        collect(doc - segBase);  // per-seg collectors
        counts[arrIdx]++;
      }
    }


    // TODO: missing should perhaps be mutually exclusive with facet.prefix???

    if (startTermIndex == -1) {
      missingCount = counts[0];
    }

    // IDEA: we could also maintain a count of "other"... everything that fell outside
    // of the top 'N'

    int off=offset;
    int lim=limit>=0 ? limit : Integer.MAX_VALUE;

    int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
    maxsize = Math.min(maxsize, nTerms);

    PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxsize) {
      final SlotAcc acc = sortAcc;
      final int mul = sortMul;
      @Override
      protected boolean lessThan(Slot a, Slot b) {
        int cmp = acc.compare(a.slot, b.slot) * mul;
        return cmp == 0 ? b.slot < a.slot : cmp < 0;
      }
    };

    Slot bottom = null;
    for (int i=(startTermIndex==-1)?1:0; i<nTerms; i++) {
      // TODO: screen out mincount?  Other filters on stats?

      if (bottom != null) {
        if (sortAcc.compare(bottom.slot, i) < 0) {
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
    for (int i=collectCount-1; i>=0; i--) {
      sortedSlots[i] = queue.pop().slot;
    }

    SimpleOrderedMap<Object> globalStats = new SimpleOrderedMap<>();
    addGlobalStats(globalStats);
    res.add("stats", globalStats);

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
        br = si.lookupOrd(ord);
        val = ft.toObject(sf, br);
        // ft.indexedToReadable(br, charsRef);
        // val = charsRef.toString();
      }

      bucket.add("val", val);

      addStats(bucket, slotNum);

      bucketList.add(bucket);

      addSubFacets(bucket, null, new TermQuery(new Term(fieldName, br.clone())) );  // TODO: missing query...
    }



    return res;
  }

  public SimpleOrderedMap<Object> getUninvertedCounts(DocSet baseDocs, String field, int offset, int limit, int mincount, boolean missing, String prefix, boolean unique) throws IOException {
    try {
      return _getUninvertedCounts(baseDocs, field, offset, limit, mincount, missing, prefix, unique);
    } finally {
      clear();
    }
  }

  public SimpleOrderedMap<Object> _getUninvertedCounts(DocSet baseDocs, String field, int offset, int limit, int mincount, boolean missing, String prefix, boolean unique) throws IOException {
    UnInvertedField uif = UnInvertedField.getUnInvertedField(field, simpleFacets.searcher);
    return uif.getCounts(this, baseDocs, offset, limit, mincount, missing, prefix, unique);
  }


  public void addSubFacets(SimpleOrderedMap<Object> bucket, DocSet base, Query bucketQuery) throws IOException {
    if (subFacets == null) return;

    SimpleFacets subSimple = null;
    DocSet newBaseSet = base;
    if (newBaseSet == null) {
      newBaseSet = simpleFacets.searcher.getDocSet(bucketQuery, simpleFacets.docs);
    }

    try {
      subSimple = new SimpleFacets(simpleFacets, newBaseSet);

      for (SimpleFacets.Subfacet sub : subFacets) {
        try {
          if ("field".equals(sub.type)) {
            subSimple.getFieldFacets(sub.value, bucket);
          } else if ("query".equals(sub.type)) {
            subSimple.getQueryFacet(sub.value, bucket, null);
          } else if ("range".equals(sub.type)) {
            subSimple.getFacetRangeCounts(sub.value, bucket, null);
          }

        } finally {
          subSimple.cleanup();
        }
      }
    } finally {
      if (newBaseSet != base) {
        newBaseSet.decref();
      }
    }
  }



}
