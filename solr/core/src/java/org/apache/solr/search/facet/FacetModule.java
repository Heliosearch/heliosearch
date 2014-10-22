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

import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SyntaxError;
import org.noggit.ObjectBuilder;

public class FacetModule {
  public static FacetRequest getFacetRequest(SolrQueryRequest req) {
    // String[] jsonFacets = req.getParams().getParams("json.facet");
    // TODO: allow multiple

    String jsonFacet = req.getParams().get("json.facet"); // TODO... allow just "facet" also?
    if (jsonFacet == null) {
      return null;
    }

    // check if facets are specifically disabled
    boolean facetsEnabled = req.getParams().getBool("facet", true);
    if (!facetsEnabled) {
      return null;
    }

    Object facetArgs = null;
    try {
      facetArgs = ObjectBuilder.fromJSON(jsonFacet);
    } catch (IOException e) {
      // should be impossible
     // TODO: log
    }

    FacetParser parser = new FacetTopParser(req);
    try {
      FacetRequest facetReq = parser.parse(facetArgs);
      return facetReq;
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
    }
  }


  public static boolean doFacets(ResponseBuilder rb) throws IOException {
    FacetRequest freq = getFacetRequest(rb.req);
    if (freq == null) return false;

    FacetContext fcontext = new FacetContext();
    fcontext.base = rb.getResults().docSet;
    fcontext.req = rb.req;
    fcontext.searcher = rb.req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);


    FacetProcessor fproc = freq.createFacetProcessor(fcontext);
    fproc.process();
    rb.rsp.add("facets", fproc.getResponse());
    return true;
  }

}
