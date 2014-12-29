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
package org.apache.solr.search.join;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RefCounted;


public class JoinQParserPlugin extends QParserPlugin {
  public static final String NAME = "join";

  @Override
  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fromField = getParam("from");
        String fromIndex = getParam("fromIndex");
        String toField = getParam("to");
        String v = localParams.get("v");
        Query fromQuery;
        long fromCoreOpenTime = 0;

        if (fromIndex != null && !fromIndex.equals(req.getCore().getCoreDescriptor().getName()) ) {
          CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();

          final SolrCore fromCore = container.getCore(fromIndex);
          RefCounted<SolrIndexSearcher> fromHolder = null;

          if (fromCore == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
          }

          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
          try {
            QParser parser = QParser.getParser(v, "lucene", otherReq);
            fromQuery = parser.getQuery();
            fromHolder = fromCore.getRegisteredSearcher();
            if (fromHolder != null) fromCoreOpenTime = fromHolder.get().getOpenTime();
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
        } else {
          QParser fromQueryParser = subQuery(v, null);
          fromQuery = fromQueryParser.getQuery();
        }

        JoinQuery jq = new JoinQuery(fromField, toField, fromIndex, fromQuery);
        jq.fromCoreOpenTime = fromCoreOpenTime;
        return jq;
      }
    };
  }
}



