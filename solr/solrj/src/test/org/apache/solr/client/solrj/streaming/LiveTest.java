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

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import static org.apache.solr.client.solrj.SolrServer.*;

import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;


public class LiveTest extends TestCase {
  public static SolrServer server;

  // Use this method as a launch point to talk to a live server
  // Uncomment the methods in the body that you want to actually run.
  public void testLive() throws Exception {
    // doUpdateAndQuery();
    // doRawResponse();
    // doStreamingResponse();
  }

  public void doUpdateAndQuery() throws Exception {
    String addr = "http://127.0.0.1:8983/solr";

    HttpSolrServer sserver = new HttpSolrServer(addr, null, new BinaryResponseParser());
    server = sserver;

    server.add( sdoc("id","doc1", "foo_s","myval") );
    server.add( sdoc("id","doc2", "foo_s","myval2", "num_d",1.414) );
    server.commit();

    QueryResponse rsp = server.query( params("q", "id:doc1") );
    System.out.println("RESPONSE: " + rsp);

    server.shutdown();
  }

  public void doRawResponse() throws Exception {
    String addr = "http://127.0.0.1:8983/solr";

    HttpSolrServer sserver = new HttpSolrServer(addr);  // generic server
    server = sserver;

    QueryRequest query = new QueryRequest( params("q", "*:*") );
    query.setResponseParser(new InputStreamResponseParser("json"));

    NamedList<Object> genericResponse = server.request(query);

    InputStream stream = (InputStream)genericResponse.get("stream");

    System.out.println("Generic Response: " + genericResponse);

    StringWriter writer = new StringWriter();
    IOUtils.copy(stream, writer);
    String output = writer.toString();
    stream.close();

    System.out.println("RESPONSE STREAM: " + output);  // TODO: this is coming out as XML???  Some sort of default?

    server.shutdown();
  }

  public void doStreamingResponse() throws Exception {
    String addr = "http://127.0.0.1:8983/solr";
    HttpSolrServer sserver = new HttpSolrServer(addr);  // generic server
    server = sserver;

    JSONTupleStream tstream = JSONTupleStream.create(server, params("q","*:*"));
    for (;;) {
      Map<String,Object> o = tstream.next();
      if (o == null) break;
      System.out.println("GOT NEXT TUPLE:" + o);
    }
    tstream.close();

    server.shutdown();
  }


  public void testTemporary() throws Exception  {
    String example="{\"responseHeader\":{\"status\":0,\"QTime\":1,\"params\":{\"q\":\"*:*\",\"wt\":\"json\",\"version\":\"2.2\"}},\"response\":{\"numFound\":2,\"start\":0,\"docs\":[{\"id\":\"doc1\",\"foo_s\":\"myval\",\"_version_\":1481340668029698048},{\"id\":\"doc2\",\"foo_s\":\"myval2\",\"num_d\":1.414,\"_version_\":1481340668094709760}]}}\n";
    JSONTupleStream tstream = new JSONTupleStream(new StringReader(example));
    for (;;) {
      Map<String,Object> o = tstream.next();
      if (o == null) break;
      System.out.println("GOT NEXT TUPLE:" + o);
    }
  }
}
