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
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class DocSetUtil {

  public static DocSet createDocSet(QueryContext queryContext, Filter filter) throws IOException {
    IndexSearcher searcher = queryContext.searcher();
    int maxDoc = searcher.getIndexReader().maxDoc();

    if (filter instanceof SolrFilter) {
      ((SolrFilter) filter).createWeight(queryContext, queryContext.indexSearcher());
    }

    try ( DocSetCollector collector = new DocSetCollector(maxDoc, (maxDoc>>6)+5) ) {

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


}
