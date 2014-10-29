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
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SyntaxError;
import org.noggit.ObjectBuilder;

public class FacetModule {

 public static void process(ResponseBuilder rb) throws IOException {
   // if this is null, faceting is not enabled
   Object componentState = rb.componentInfo.get(FacetComponentState.class);
   if (componentState == null) return;

   FacetComponentState facetState = (FacetComponentState)componentState;

    FacetContext fcontext = new FacetContext();
    fcontext.base = rb.getResults().docSet;
    fcontext.req = rb.req;
    fcontext.searcher = rb.req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);


    FacetProcessor fproc = facetState.facetRequest.createFacetProcessor(fcontext);
    fproc.process();
    rb.rsp.add("facets", fproc.getResponse());
  }


  public static void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();

    String[] jsonFacets = rb.req.getParams().getParams("json.facet");
    if (jsonFacets == null) return;

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    if (isShard) {
      String jfacet = params.get("jfacet");
      if (jfacet == null) {
        // if this is a shard request, but there is no jfacet state, then don't do anything.
        return;
      }
    }

    // At this point, we know we need to do something.  Create and save the state.
    rb.setNeedDocSet( true );


    Map<String,Object> all = null;
    for (String jsonFacet : jsonFacets) {
      Map<String,Object> facetArgs = null;
      try {
        facetArgs = (Map<String,Object>)ObjectBuilder.fromJSON(jsonFacet);
      } catch (IOException e) {
        // impossible
      }

      if (all == null) {
        all = facetArgs;
      } else {
        all.putAll( facetArgs );
      }
    }

    // Parse the facet in the prepare phase?
    FacetParser parser = new FacetTopParser(rb.req);
    FacetRequest facetRequest = null;
    try {
      facetRequest = parser.parse(all);
    } catch (SyntaxError syntaxError) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, syntaxError);
    }

    FacetComponentState fcState = new FacetComponentState();
    fcState.rb = rb;
    fcState.isShard = isShard;
    fcState.facetCommands = all;
    fcState.facetRequest = facetRequest;

    rb.componentInfo.put(FacetComponentState.class, fcState);
  }

}

// TODO: make public
class FacetComponentState {
  ResponseBuilder rb;
  Map<String,Object> facetCommands;
  FacetRequest facetRequest;
  boolean isShard;
}
