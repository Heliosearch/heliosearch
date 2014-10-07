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

/*
  Queries a Solr instance, and maps SolrDocs to Tuples.
  Initial version works with the json format and only SolrDocs are handled.
*/

public class SolrStream extends TupleStream {

  private String baseUrl;
  private Map params;
  private int worker;
  private String[] partitionKeys;

  public SolrStream(String baseUrl, Map params) {
    this.baseUrl = baseUrl;
    this.params = params;
  }

  public SolrStream(String baseUrl, Map params, int workers, String[] partitionKeys) {
    super(workers, partitionKeys);
    this.baseUrl = baseUrl;
    this.params = params;
  }

  public List<TupleStream> children() {
    return new ArrayList();
  }

  public void setWorker(int worker) {
    this.worker = worker;
  }

  public void open() throws IOException {
    if(this.workers != 0) {
      //Add the parameter for the partitioner
    }
  }

  public void close() throws IOException {

  }

  public Tuple read() throws IOException {
    return new Tuple(true);
  }
}