package org.apache.solr.search.join;

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
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.HS;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.HashDocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;
import org.apache.solr.search.SortedIntDocSetNative;

class TermJoinImpl extends JoinQuery.JoinImpl {
  DocSet resultSet;
  Filter filter;

  // debugging stats
  long create_time;
  long resultListDocs;      // total number of docs collected
  int fromTermCount;
  long fromTermTotalDf;
  int fromTermDirectCount;  // number of fromTerms that were too small to use the filter cache
  int fromTermHits;         // number of fromTerms that intersected the from query
  long fromTermHitsTotalDf; // sum of the df of the matching terms
  int toTermHits;           // num if intersecting from terms that match a term in the to field
  long toTermHitsTotalDf;   // sum of the df for the toTermHits
  int toTermDirectCount;    // number of toTerms that we set directly on a bitset rather than doing set intersections
  int smallSetsDeferred;    // number of small sets collected to be used later to intersect w/ bitset or create another small set


  public TermJoinImpl(JoinQuery q, JoinQuery.JoinWeight weight) {
    super(q, weight);
  }

  // TODO: make this add to base class debug... / weight debug
  @Override
  public void debug() {
    if (w.debug) { // TODO: the debug process itself causes the query to be re-executed and this info is added multiple times!
      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<Object>();
      dbg.add("time", create_time);
      dbg.add("fromSetSize", w.fromSetSize);  // the input
      dbg.add("toSetSize", resultSet.size());    // the output

      dbg.add("fromTermCount", fromTermCount);
      dbg.add("fromTermTotalDf", fromTermTotalDf);
      dbg.add("fromTermDirectCount", fromTermDirectCount);
      dbg.add("fromTermHits", fromTermHits);
      dbg.add("fromTermHitsTotalDf", fromTermHitsTotalDf);
      dbg.add("toTermHits", toTermHits);
      dbg.add("toTermHitsTotalDf", toTermHitsTotalDf);
      dbg.add("toTermDirectCount", toTermDirectCount);
      dbg.add("smallSetsDeferred", smallSetsDeferred);
      dbg.add("toSetDocsAdded", resultListDocs);

      // TODO: perhaps synchronize  addDebug in the future...
      w.rb.addDebug(dbg, "join", q.toString());
    }
  }

  @Override
  public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    if (filter == null) {
      boolean debug = w.rb != null && w.rb.isDebug();
      long start = debug ? System.currentTimeMillis() : 0;
      resultSet = getDocSet();
      long end = debug ? System.currentTimeMillis() : 0;
      create_time = end - start;

      debug();  // for now...

      filter = resultSet.getTopFilter();
    }

    // Although this set only includes live docs, other filters can be pushed down to queries.
    DocIdSet readerSet = filter.getDocIdSet(context, acceptDocs);
    return new DocIdSetScorer(this.w, readerSet == null ? DocIdSetIterator.empty() : readerSet.iterator(), q.getBoost());
  }




  public DocSet getDocSet() throws IOException {
    FixedBitSet resultBits = null;

    // minimum docFreq to use the cache
    int minDocFreqFrom = Math.max(5, w.fromSearcher.maxDoc() >> 13);
    int minDocFreqTo = Math.max(5, w.toSearcher.maxDoc() >> 13);

    // use a smaller size than normal since we will need to sort and dedup the results
    int maxSortedIntSize = Math.max(10, w.toSearcher.maxDoc() >> 10);

    DocSet fromSet = w.fromSet;

    LinkedList<DocSet> resultList = new LinkedList<>();
    try {

      // make sure we have a set that is fast for random access, if we will use it for that
      DocSet fastForRandomSet = fromSet;
      if (minDocFreqFrom > 0 && fromSet instanceof SortedIntDocSetNative) {
        SortedIntDocSetNative sset = (SortedIntDocSetNative) fromSet;
        fastForRandomSet = new HashDocSet(sset.getIntArrayPointer(), 0, sset.size(), HashDocSet.DEFAULT_INVERSE_LOAD_FACTOR);
      }

      Fields fromFields = w.fromSearcher.getAtomicReader().fields();
      Fields toFields = w.fromSearcher == w.toSearcher ? fromFields : w.toSearcher.getAtomicReader().fields();
      if (fromFields == null) return DocSet.EMPTY;
      Terms fromTerms = fromFields.terms(q.fromField);
      Terms toTerms = toFields.terms(q.toField);
      if (fromTerms == null || toTerms == null) return DocSet.EMPTY;
      String prefixStr = TrieField.getMainValuePrefix(w.fromSearcher.getSchema().getFieldType(q.fromField));
      BytesRef prefix = prefixStr == null ? null : new BytesRef(prefixStr);

      BytesRef term = null;
      TermsEnum fromTermsEnum = fromTerms.iterator(null);
      TermsEnum toTermsEnum = toTerms.iterator(null);
      SolrIndexSearcher.DocsEnumState fromDeState = null;
      SolrIndexSearcher.DocsEnumState toDeState = null;

      if (prefix == null) {
        term = fromTermsEnum.next();
      } else {
        if (fromTermsEnum.seekCeil(prefix) != TermsEnum.SeekStatus.END) {
          term = fromTermsEnum.term();
        }
      }

      Bits fromLiveDocs = w.fromSearcher.getAtomicReader().getLiveDocs();
      Bits toLiveDocs = w.fromSearcher == w.toSearcher ? fromLiveDocs : w.toSearcher.getAtomicReader().getLiveDocs();

      fromDeState = new SolrIndexSearcher.DocsEnumState();
      fromDeState.fieldName = q.fromField;
      fromDeState.liveDocs = fromLiveDocs;
      fromDeState.termsEnum = fromTermsEnum;
      fromDeState.docsEnum = null;
      fromDeState.minSetSizeCached = minDocFreqFrom;

      toDeState = new SolrIndexSearcher.DocsEnumState();
      toDeState.fieldName = q.toField;
      toDeState.liveDocs = toLiveDocs;
      toDeState.termsEnum = toTermsEnum;
      toDeState.docsEnum = null;
      toDeState.minSetSizeCached = minDocFreqTo;

      while (term != null) {
        if (prefix != null && !StringHelper.startsWith(term, prefix))
          break;

        fromTermCount++;

        boolean intersects = false;
        int freq = fromTermsEnum.docFreq();
        fromTermTotalDf++;

        if (freq < minDocFreqFrom) {
          fromTermDirectCount++;
          // OK to skip liveDocs, since we check for intersection with docs matching query
          fromDeState.docsEnum = fromDeState.termsEnum.docs(null, fromDeState.docsEnum, DocsEnum.FLAG_NONE);
          DocsEnum docsEnum = fromDeState.docsEnum;

          if (docsEnum instanceof MultiDocsEnum) {
            MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum) docsEnum).getSubs();
            int numSubs = ((MultiDocsEnum) docsEnum).getNumSubs();
            outer:
            for (int subindex = 0; subindex < numSubs; subindex++) {
              MultiDocsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.docsEnum == null) continue;
              int base = sub.slice.start;
              int docid;
              while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid + base)) {
                  intersects = true;
                  break outer;
                }
              }
            }
          } else {
            int docid;
            while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (fastForRandomSet.exists(docid)) {
                intersects = true;
                break;
              }
            }
          }
        } else {
          // use the filter cache
          DocSet fromTermSet = w.fromSearcher.getDocSet(fromDeState);
          intersects = fromSet.intersects(fromTermSet);
          fromTermSet.decref();
        }

        if (intersects) {
          fromTermHits++;
          fromTermHitsTotalDf++;
          TermsEnum.SeekStatus status = toTermsEnum.seekCeil(term);
          if (status == TermsEnum.SeekStatus.END) break;
          if (status == TermsEnum.SeekStatus.FOUND) {
            toTermHits++;
            int df = toTermsEnum.docFreq();
            toTermHitsTotalDf += df;
            if (resultBits == null && df + resultListDocs > maxSortedIntSize && resultList.size() > 0) {
              resultBits = new FixedBitSet(w.toSearcher.maxDoc());
            }

            // if we don't have a bitset yet, or if the resulting set will be too large
            // use the filterCache to get a DocSet
            if (toTermsEnum.docFreq() >= minDocFreqTo || resultBits == null) {
              // use filter cache
              DocSet toTermSet = w.toSearcher.getDocSet(toDeState);
              resultListDocs += toTermSet.size();
              if (resultBits != null) {
                toTermSet.setBitsOn(resultBits);
                toTermSet.decref();
              } else {
                if (toTermSet instanceof BitDocSetNative) {
                  resultBits = ((BitDocSetNative) toTermSet).toFixedBitSet();
                  toTermSet.decref();
                } else if (toTermSet instanceof BitDocSet) {
                  // shouldn't happen any more?
                  resultBits = toTermSet.getBits().clone();
                } else {
                  // should be SortedIntDocSetNative
                  resultList.add(toTermSet);
                }
              }
            } else {
              toTermDirectCount++;

              // need to use liveDocs here so we don't map to any deleted ones
              toDeState.docsEnum = toDeState.termsEnum.docs(toDeState.liveDocs, toDeState.docsEnum, DocsEnum.FLAG_NONE);
              DocsEnum docsEnum = toDeState.docsEnum;

              if (docsEnum instanceof MultiDocsEnum) {
                MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum) docsEnum).getSubs();
                int numSubs = ((MultiDocsEnum) docsEnum).getNumSubs();
                for (int subindex = 0; subindex < numSubs; subindex++) {
                  MultiDocsEnum.EnumWithSlice sub = subs[subindex];
                  if (sub.docsEnum == null) continue;
                  int base = sub.slice.start;
                  int docid;
                  while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    resultListDocs++;
                    resultBits.set(docid + base);
                  }
                }
              } else {
                int docid;
                while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  resultListDocs++;
                  resultBits.set(docid);
                }
              }
            }

          }
        }

        term = fromTermsEnum.next();
      }

      smallSetsDeferred = resultList.size();

      if (resultBits != null) {

        for (; ; ) {
          DocSet set = resultList.pollFirst();
          if (set == null) break;
          set.setBitsOn(resultBits);
          set.decref();
        }
        return new BitDocSet(resultBits);
      }

      if (resultList.size() == 0) {
        return DocSet.EMPTY;
      }

      /** This could be off-heap, and we don't want to have to try and free it later
       if (resultList.size() == 1) {
       return resultList.get(0);
       }
       **/

      int sz = 0;

      for (DocSet set : resultList)
        sz += set.size();

      int[] docs = new int[sz];
      int pos = 0;

      for (; ; ) {
        DocSet set = resultList.pollFirst();
        if (set == null) break;
        if (set instanceof SortedIntDocSet) {
          System.arraycopy(((SortedIntDocSet) set).getDocs(), 0, docs, pos, set.size());
        } else {
          HS.copyInts(((SortedIntDocSetNative) set).getIntArrayPointer(), 0, docs, pos, set.size());
        }
        pos += set.size();
        set.decref();
      }

      Arrays.sort(docs);  // TODO: try switching to timsort or something like a bucket sort for numbers...
      int[] dedup = new int[sz];
      pos = 0;
      int last = -1;
      for (int doc : docs) {
        if (doc != last)
          dedup[pos++] = doc;
        last = doc;
      }

      if (pos != dedup.length) {
        dedup = Arrays.copyOf(dedup, pos);
      }

      return new SortedIntDocSet(dedup, dedup.length);

    } finally {
      // The weight currently owns fromSet
      // fromSet.decref();
      // resultList should be empty, except if an exception happened somewhere
      for (DocSet set : resultList) {
        set.decref();
      }
    }
  }
}
