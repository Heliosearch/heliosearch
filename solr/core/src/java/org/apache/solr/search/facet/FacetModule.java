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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SyntaxError;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FacetModule extends SearchComponent {
  public static Logger log = LoggerFactory.getLogger(FacetModule.class);

  public static final String COMPONENT_NAME = "hs_facet";

  // Ensure these don't overlap with other PURPOSE flags in ShardRequest
  // The largest current flag in ShardRequest is 0x00002000
  // We'll put our bits in the middle to avoid future ones in ShardRequest and
  // custom ones that may start at the top.
  public final static int PURPOSE_GET_JSON_FACETS      = 0x00100000;
  public final static int PURPOSE_REFINE_JSON_FACETS   = 0x00200000;

  // Internal information passed down from the top level to shards for distributed faceting.
  private final static String FACET_STATE = "_facet_";

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // if this is null, faceting is not enabled
    FacetComponentState facetState = (FacetComponentState) rb.componentInfo.get(FacetComponentState.class);
    if (facetState == null) return;

    FacetContext fcontext = new FacetContext();
    fcontext.base = rb.getResults().docSet;
    fcontext.req = rb.req;
    fcontext.searcher = rb.req.getSearcher();
    fcontext.qcontext = QueryContext.newContext(fcontext.searcher);
    if (fcontext.isShard()) {
      fcontext.flags |= FacetContext.IS_SHARD;
    }

    FacetProcessor fproc = facetState.facetRequest.createFacetProcessor(fcontext);
    fproc.process();
    rb.rsp.add("facets", fproc.getResponse());
  }


  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();

    String[] jsonFacets = rb.req.getParams().getParams("json.facet");
    if (jsonFacets == null) return;

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    if (isShard) {
      String jfacet = params.get(FACET_STATE);
      if (jfacet == null) {
        // if this is a shard request, but there is no facet state, then don't do anything.
        return;
      }
    }

    // At this point, we know we need to do something.  Create and save the state.
    rb.setNeedDocSet(true);


    Map<String, Object> all = null;
    for (String jsonFacet : jsonFacets) {
      Map<String, Object> facetArgs = null;
      try {
        facetArgs = (Map<String, Object>) ObjectBuilder.fromJSON(jsonFacet);
      } catch (IOException e) {
        // impossible
      }

      if (all == null) {
        all = facetArgs;
      } else {
        all.putAll(facetArgs);
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



  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    FacetComponentState facetState = (FacetComponentState) rb.componentInfo.get(FacetComponentState.class);
    if (facetState == null) return ResponseBuilder.STAGE_DONE;

    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who,ShardRequest sreq) {
    FacetComponentState facetState = (FacetComponentState) rb.componentInfo.get(FacetComponentState.class);
    if (facetState == null) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= FacetModule.PURPOSE_GET_JSON_FACETS;
      sreq.params.set(FACET_STATE, "{}");
    } else {
      // turn off faceting on other requests
      sreq.params.remove("json.facet");
      sreq.params.remove(FACET_STATE);
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    FacetComponentState facetState = (FacetComponentState) rb.componentInfo.get(FacetComponentState.class);
    if (facetState == null) return;

    for (ShardResponse shardRsp : sreq.responses) {
      SolrResponse rsp = shardRsp.getSolrResponse();
      NamedList<Object> top = rsp.getResponse();
      Object facet = top.get("facets");
      if (facet == null) continue;
      if (facetState.merger == null) {
        facetState.merger = facetState.facetRequest.createFacetMerger(facet);
      }
      facetState.merger.merge(facet);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;

    FacetComponentState facetState = (FacetComponentState) rb.componentInfo.get(FacetComponentState.class);
    if (facetState == null) return;

    if (facetState.merger != null) {
      rb.rsp.add("facets", facetState.merger.getMergedResult());
    }
  }

  @Override
  public String getDescription() {
    return "Heliosearch Faceting";
  }

  @Override
  public String getSource() {
    return null;
  }

}


class FacetComponentState {
  ResponseBuilder rb;
  Map<String,Object> facetCommands;
  FacetRequest facetRequest;
  boolean isShard;

  //
  // Only used for distributed search
  //
  FacetMerger merger;
}

class FacetMerger {
  public void merge(Object facetResult) {

  }

  public Object getMergedResult() {
    return null; // TODO
  }
}

class DoubleFacetMerger {
   double val;

}


// base class for facets that create buckets (and can hence have sub-facets)
class FacetBucketMerger<FacetRequestT extends FacetRequest> extends FacetMerger {
  FacetRequestT freq;

  public FacetBucketMerger(FacetRequestT freq) {
    this.freq = freq;
  }

  FacetBucket newBucket(Object bucketVal) {
    return new FacetBucket(this, bucketVal);
  }

  // do subs...

  // callback stuff for buckets?
  // passing object gives us a chance to specialize based on value
  FacetMerger createFacetMerger(String key, Object val) {
    FacetRequest sub = freq.getSubFacets().get(key);
    if (sub != null) {
      return sub.createFacetMerger(val);
    }

    AggValueSource subStat = freq.getFacetStats().get(key);
    if (subStat != null) {
      return subStat.createFacetMerger(val);
    }

    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no merger for key=" + key + " , val=" + val);
  }
}

class FacetQueryMerger extends FacetBucketMerger<FacetQuery> {
  FacetBucket bucket;

  public FacetQueryMerger(FacetQuery freq) {
    super(freq);
  }

  @Override
  public void merge(Object facet) {
    if (bucket == null) {
      bucket = newBucket(null);
    }
    bucket.mergeBucket((Map<String, Object>) facet);
  }

  @Override
  public Object getMergedResult() {
    return bucket.getMergedBucket();
  }
}



class FacetBucket {
  FacetBucketMerger parent;
  Object bucketValue;
  long count;
  Map<String, FacetMerger> subs;

  public FacetBucket(FacetBucketMerger parent, Object bucketValue) {
    this.bucketValue = bucketValue;
    // todo... create subs?
  }

  public long getCount() {
    return count;
  }

  public void mergeBucket(Map<String, Object> bucket) {
    // todo: for refinements, we want to recurse, but not re-do stats for intermediate buckets

    long singleCount = ((Number)bucket.get("count")).longValue();
    count += singleCount;

    // drive merging off the received bucket?
    for (Map.Entry<String,Object> entry : bucket.entrySet()) {
      String key = entry.getKey();

      Object val = entry.getValue();

      FacetMerger merger = subs.get(key);
      // TODO: Create all mergers at instantiation time, or ask our parent to create a Merger?
      if (merger == null) {
        parent.createFacetMerger(key, val);



        // check for expected info w/o merger
        if ("count".equals(key)) continue;
        // what about "val"?

        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no merger for key=" + key + " , val=" + val);
      }

      merger.merge(val);
    }
  }


  public Map<String,Object> getMergedBucket() {
    Map<String,Object> out = new LinkedHashMap<String,Object>( (subs == null ? 0 : subs.size()) + 2);
    if (bucketValue != null) {
      out.put("val", bucketValue);
    }
    out.put("count", count);
    if (subs != null) {
      for (Map.Entry<String,FacetMerger> mergerEntry : subs.entrySet()) {
        FacetMerger subMerger = mergerEntry.getValue();
        out.put( mergerEntry.getKey(), subMerger.getMergedResult() );
      }
    }

    return out;
  }
}



class FacetFieldMerger extends FacetBucketMerger<FacetField> {
  FacetField freq;
  FacetBucket missingBucket;
  FacetBucket allBuckets;

  LinkedHashMap<Object,FacetBucket> buckets = new LinkedHashMap<Object,FacetBucket>();
  List<FacetBucket> sortedBuckets;

  public FacetFieldMerger(FacetField freq) {
    super(freq);
  }

  public void merge(Map<String, Object> facetResult) {
    if (freq.missing) {
      if (missingBucket == null) {
        missingBucket = newBucket(null);
      }
      missingBucket.mergeBucket( (Map<String,Object>) facetResult.get("missing") );
    }

    if (freq.allBuckets) {
      if (allBuckets == null) {
        allBuckets = newBucket(null);
      }
      allBuckets.mergeBucket( (Map<String,Object>) facetResult.get("allBuckets") );
    }

    List<Map<String,Object>> bucketList = (List<Map<String,Object>>) facetResult.get("buckets");

    mergeBucketList(bucketList);
  }

  public void mergeBucketList(List<Map<String,Object>> bucketList) {
    for (Map<String,Object> bucketRes : bucketList) {
      Object bucketVal = bucketRes.get("val");
      FacetBucket bucket = buckets.get(bucketVal);
      if (bucket == null) {
        bucket = newBucket(bucketVal);
        buckets.put(bucketVal, bucket);
      }
      bucket.mergeBucket( bucketRes );
    }
  }

  public void sortBuckets() {
    sortedBuckets = new ArrayList<>( buckets.values() );


  }

  @Override
  public Object getMergedResult() {
    Map<String,Object> result = new LinkedHashMap<>();

    sortBuckets();

    int first = (int)freq.offset;
    int end = freq.limit >=0 ? first + (int) freq.limit : Integer.MAX_VALUE;
    int last = Math.min(sortedBuckets.size(), end);

    List<Map<String,Object>> resultBuckets = new ArrayList<>(Math.max(0, (last - first)));
    for (int i=first; i<last; i++) {
      FacetBucket bucket = sortedBuckets.get(i);
      resultBuckets.add( bucket.getMergedBucket() );
    }


    result.put("buckets", resultBuckets);
    if (missingBucket != null) {
      result.put( "missing", missingBucket.getMergedBucket() );
    }
    if (allBuckets != null) {
      result.put( "allBuckets", missingBucket.getMergedBucket() );
    }

    return result;
  }
}
