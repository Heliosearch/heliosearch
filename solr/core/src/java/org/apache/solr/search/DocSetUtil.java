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
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.request.SolrRequestInfo;

import java.io.IOException;

public class DocSetUtil {

  private static DocSet createDocSetByIterator(QueryContext queryContext, Filter filter) throws IOException {
    IndexSearcher searcher = queryContext.searcher();
    int maxDoc = searcher.getIndexReader().maxDoc();

    if (filter instanceof SolrFilter) {
      ((SolrFilter) filter).createWeight(queryContext, queryContext.indexSearcher());
    }

    try ( DocSetCollector collector = new DocSetCollector((maxDoc>>6)+5, maxDoc) ) {

      for (AtomicReaderContext readerContext : searcher.getIndexReader().getContext().leaves()) {
        collector.setNextReader(readerContext);
        Bits acceptDocs = readerContext.reader().getLiveDocs();

        DocIdSet docIdSet = filter instanceof SolrFilter
            ? ((SolrFilter)filter).getDocIdSet(queryContext, readerContext, acceptDocs )
            : filter.getDocIdSet(readerContext, acceptDocs);

        if (docIdSet == null) continue;
        DocIdSetIterator iter = docIdSet.iterator();

        for (;;) {
          int id = iter.nextDoc();
          if (id == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          collector.collect(id);
        }

      }

      return collector.getDocSet();
    }
  }

  public static DocSet createDocSet(QueryContext queryContext, Filter filter) throws IOException {
    if (filter instanceof DocSetProducer) {
      return ((DocSetProducer) filter).createDocSet(queryContext);
    } else {
      return createDocSetByIterator(queryContext, filter);
    }
  }


   public static DocSet createDocSet(QueryContext queryContext, Query query) throws IOException {
     IndexSearcher searcher = queryContext.searcher();
     int maxDoc = searcher.getIndexReader().maxDoc();

     try ( DocSetCollector collector = new DocSetCollector((maxDoc>>6)+5, maxDoc) ) {
       queryContext.searcher().search(query, null, collector);
       return collector.getDocSet();
     }
   }


 }
