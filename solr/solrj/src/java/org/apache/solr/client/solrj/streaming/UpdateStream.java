package org.apache.solr.client.solrj.streaming;

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
import java.util.List;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.SolrServer;

public class UpdateStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream source;
  private String collection;
  private String zkHost;
  private StreamContext context;
  private ArrayBlockingQueue<List<SolrInputDocument>> indexingQueue = new ArrayBlockingQueue<List<SolrInputDocument>>(10,true);private int batchSize;
  private SolrServer server;
  private int batchCount;
  private Indexer indexer;
  private String updateUrl;

  public UpdateStream(String zkHost,
                      String collection,
                      int batchSize,
                      TupleStream source) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.batchSize = batchSize;
    this.source = source;
  }

  public UpdateStream(String updateUrl,
                      int batchSize,
                      TupleStream source) {
    this.batchSize = batchSize;
    this.source = source;
    this.updateUrl = updateUrl;
  }

  public void setStreamContext(StreamContext context) {
    this.context = context;
  }

  public List<TupleStream> children() {
    List<TupleStream> children = new ArrayList();
    children.add(source);
    return children;
  }

  public void open() throws IOException {
    if(zkHost != null) {
      CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
      cloudSolrServer.setDefaultCollection(this.collection);
      this.server = cloudSolrServer;
    } else {
      //Use HttpSolrServer
    }

    this.indexer = new Indexer();
    indexer.start();
  }

  public void close() throws IOException {
    source.close();
  }

  public Tuple read() throws IOException {
    List<SolrInputDocument> docs = new ArrayList();
    while(true) {
      Tuple tuple = source.read();

      if(!tuple.EOF) {
        docs.add(tupleToDoc(tuple));
        if (docs.size() == batchSize) {
          indexingQueue.offer(docs);
          ++batchCount;
          return new Tuple(new HashMap());
        }
      } else {
        indexingQueue.offer(docs);
        if(docs.size() > 0) {
          //Add empty array to shutdown the indexingThread.
          indexingQueue.offer(new ArrayList<SolrInputDocument>());
        }
        ++batchCount;
        return tuple;
      }
    }
  }

  private SolrInputDocument tupleToDoc(Tuple tuple) {
    SolrInputDocument doc = new SolrInputDocument();
    Iterator<Entry> it =  tuple.fields.entrySet().iterator();
    while(it.hasNext()) {
      Entry entry = it.next();
      String key = (String)entry.getKey();
      Object value = entry.getValue();
      if(value instanceof String) {
        doc.addField(key, value);
      } else if(value instanceof Number) {
        doc.addField(key, value.toString());
      } else if(value instanceof List) {
        List l = (List)value;
        for(Object o : l)
          doc.addField(key, o.toString());
      }
    }
    return doc;
  }

  private class Indexer extends Thread {

    public void run() {
      while(true) {
        try {
          List<SolrInputDocument> docs = indexingQueue.take();
          if(docs.size() > 0) {
            UpdateRequest request = new UpdateRequest();
            request.add(docs);
            request.process(server);
          } else {
            break;
          }

        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }
}