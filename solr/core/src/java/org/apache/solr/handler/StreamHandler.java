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

package org.apache.solr.handler;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import org.apache.solr.client.solrj.streaming.TupleStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.params.SolrParams;
import sun.misc.UUDecoder;


public class StreamHandler extends RequestHandlerBase {

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    String uuStream = params.get("stream");

    UUDecoder uuDecoder = new UUDecoder();
    byte[] bytes = uuDecoder.decodeBuffer(uuStream);
    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    ObjectInputStream objectInputStream = new ObjectInputStream(byteStream);
    TupleStream tupleStream = (TupleStream)objectInputStream.readObject();
    int worker = params.getInt("workerID");
    int numWorkers = params.getInt("numWorkers");
    tupleStream.setWorkers(numWorkers, worker);
    req.getContext().put(TupleStream.class, tupleStream);
  }

  public String getDescription() {
    return "StreamHandler";
  }

  public String getSource() {
    return null;
  }
}