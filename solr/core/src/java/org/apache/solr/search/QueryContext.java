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

import org.apache.lucene.search.IndexSearcher;

import java.util.IdentityHashMap;

public class QueryContext  {
  private IdentityHashMap map;
  private final SolrIndexSearcher searcher;
  private final IndexSearcher indexSearcher;


  // migrated from ValueSource
  public static QueryContext newContext(IndexSearcher searcher) {
    QueryContext context = new QueryContext(searcher);
    return context;
  }


  public QueryContext(IndexSearcher searcher) {
    this.searcher = searcher instanceof SolrIndexSearcher ? (SolrIndexSearcher)searcher : null;
    indexSearcher = searcher;
  }


  public SolrIndexSearcher searcher() {
    return searcher;
  }

  public IndexSearcher indexSearcher() {
    return indexSearcher;
  }

  public Object get(Object key) {
    if (map == null) return null;
    return map.get(key);
  }

  public Object put(Object key, Object val) {
    if (map == null) {
      map = new IdentityHashMap();
    }
    return map.put(key, val);
  }
}
