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
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import sun.misc.UUEncoder;

public class ParallelStream extends CloudSolrStream {

  private TupleStream tupleStream;
  private int workers;
  private transient ZkStateReader zkStateReader;
  private String encoded;

  public ParallelStream(String zkHost,
                        String collection,
                        TupleStream tupleStream,
                        int workers,
                        Comparator comp) throws IOException {
    this.zkHost = zkHost;
    this.collection = collection;
    this.workers = workers;
    this.comp = comp;
    this.tupleStream = tupleStream;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bout);
    out.writeObject(tupleStream);
    byte[] bytes = bout.toByteArray();
    UUEncoder uu = new UUEncoder();
    this.encoded = uu.encode(bytes);
  }

  public List<TupleStream> children() {
    List l = new ArrayList();
    l.add(tupleStream);
    return l;
  }

  protected void constructStreams() throws IOException {

    try {
      zkStateReader = connect();
      ClusterState clusterState = zkStateReader.getClusterState();
      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);
      long time = System.currentTimeMillis();
      int workerNum = 0;
      for(Slice slice : slices) {
        HashMap params = new HashMap();

        params.put("distrib","false"); // We are the aggregator.
        params.put("numWorkers", workers);
        params.put("workID", workerNum);
        params.put("stream", this.encoded);

        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList();
        for(Replica replica : replicas) {
          shuffler.add(replica);
        }

        Collections.shuffle(shuffler, new Random(time));
        Replica rep = shuffler.get(0);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        SolrStream solrStream = new SolrStream(url, params);
        solrStreams.add(solrStream);
        ++workerNum;
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
