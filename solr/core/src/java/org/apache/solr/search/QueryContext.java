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
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.field.FieldValues;
import org.apache.solr.search.field.TopValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class QueryContext implements Closeable {
  private IdentityHashMap map;
  private final SolrIndexSearcher searcher;
  private final IndexSearcher indexSearcher;
  private List<Closeable> closeHooks;
  private Map<String,TopValues> topValues;

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

  public void addCloseHook(Closeable closeable) {
    if (closeHooks == null) {
      closeHooks = new ArrayList<>();
      // for now, defer closing until the end of the request
      SolrRequestInfo.getRequestInfo().addCloseHook(this);
    }

    closeHooks.add(closeable);
  }

  /** Don't call close explicitly!  This will be automatically closed at the end of the request */
  @Override
  public void close() throws IOException {
    if (closeHooks != null) {
      for (Closeable hook : closeHooks) {
        try {
          hook.close();
        } catch (Exception e) {
          SolrException.log(SolrCore.log, "Exception during close hook", e);
        }
      }
    }

    closeHooks = null;
    map = null;
    topValues = null;
  }


  /** This can return null */
  public TopValues getTopValues(FieldValues fvals) {
    if (topValues == null) return null;
    return topValues.get(fvals.getField().getName());
  }

  public void setTopValues(FieldValues fvals, TopValues topVals) {
    if (topValues == null) {
      topValues = new HashMap<>();
    }

    TopValues prev = topValues.put(fvals.getField().getName(), topVals);
    addCloseHook(topVals);
    // prev may be non-null if we switched from non-topVal to topVal
  }

}
