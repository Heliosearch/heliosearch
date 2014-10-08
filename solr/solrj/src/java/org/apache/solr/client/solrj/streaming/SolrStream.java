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

package org.apache.solr.client.solrj.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

/*
  Queries a Solr instance, and maps SolrDocs to Tuples.
  Initial version works with the json format and only SolrDocs are handled.
*/

public class SolrStream extends TupleStream {

  private String baseUrl;
  private Map params;
  private int numWorkers;
  private int workerID;
  private String[] partitionKeys;
  private transient JSONTupleStream jsonTupleStream;
  private transient HttpSolrServer server;

  public SolrStream(String baseUrl, Map params) {
    this.baseUrl = baseUrl;
    this.params = params;
  }

  public SolrStream(String baseUrl, Map params, String[] partitionKeys) {
    super(partitionKeys);
    this.baseUrl = baseUrl;
    this.params = params;
  }

  public List<TupleStream> children() {
    return new ArrayList();
  }

  public void setWorkers(int numWorkers, int workerID) {
    this.numWorkers = numWorkers;
    this.workerID = workerID;
  }

  public void open() throws IOException {

    server = new HttpSolrServer(baseUrl);
    try {
      jsonTupleStream = JSONTupleStream.create(server, loadParams(params));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private SolrParams loadParams(Map params) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    if(this.numWorkers > 0) {
      String partitionFilter = getPartitionFilter();
      solrParams.add("fq", partitionFilter);
    }

    Iterator<Map.Entry> it = params.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry entry = it.next();
      solrParams.add((String)entry.getKey(), (String)entry.getValue());
    }

    return solrParams;
  }

  private String getPartitionFilter() {
    StringBuilder buf = new StringBuilder("{!hash workers=");
    buf.append(this.numWorkers);
    buf.append(" worker=");
    buf.append(this.workerID);
    buf.append(" keys='");
    boolean comma = false;
    for(String key : partitionKeys) {
      if(comma) {
        buf.append(",");
      }
      buf.append(key);
      comma = true;
    }
    buf.append("'}");
    return buf.toString();
  }

  public void close() throws IOException {
    jsonTupleStream.close();
    server.shutdown();
  }

  public Tuple read() throws IOException {
    Map fields = jsonTupleStream.next();
    if(fields == null) {
      //Return the EOF tuple.
      return new Tuple(true);
    } else {
      return new Tuple(fields, false);
    }
  }
}