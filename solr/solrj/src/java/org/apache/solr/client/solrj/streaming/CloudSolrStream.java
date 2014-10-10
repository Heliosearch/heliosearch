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
import java.util.Map;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.cloud.Slice;
import org.apache.zookeeper.KeeperException;

/*
* Connects to Zookeeper to pick replicas from a specific collection to send the query to.
* SolrStream instances are used to send the query to the replicas.
* SolrStreams are opened using a Thread pool, but a single thread is used to iterate through each stream's tuples.
* Tuples are order from the underlying SolrStream using the Comparator.
*/

public class CloudSolrStream extends TupleStream {

  protected String zkHost;
  protected String collection;
  protected Map params;
  private Map<String, String> fieldMappings;
  private TreeSet<TupleWrapper> tuples;
  protected Comparator<Tuple> comp;
  protected List<TupleStream> solrStreams = new ArrayList();
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  private transient ZkStateReader zkStateReader;
  private int numWorkers;
  private int workerID;

  public CloudSolrStream(String zkHost, String collection, Map params) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.params = params;
    this.tuples = new TreeSet();
    String sort = (String)params.get("sort");
    this.comp = parseComp(sort);
  }

  public CloudSolrStream(String zkHost, String collection, Map params, String[] partitionKeys) {
    super(partitionKeys);
    this.zkHost = zkHost;
    this.collection = collection;
    this.params = params;
    this.tuples = new TreeSet();
    String sort = (String)params.get("sort");
    this.comp = parseComp(sort);
  }

  //Used by the ParallelStream
  protected CloudSolrStream() {

  }

  public void setComp(Comparator<Tuple> comp) {
    this.comp = comp;
  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public void setWorkers(int numWorkers, int workerID) {
    this.numWorkers = numWorkers;
    this.workerID = workerID;
  }

  public void open() throws IOException {
    constructStreams();
    openStreams();
    zkStateReader.close();
  }

  public List<TupleStream> children() {
    return solrStreams;
  }

  private Comparator<Tuple> parseComp(String sort) {
    String[] sorts = sort.split(",");
    Comparator[] comps = new Comparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];
      String[] spec = s.split(" ");
      if(spec[1].trim().equalsIgnoreCase("asc")) {
        comps[i] = new AscFieldComp(spec[0]);
      } else {
        comps[i] = new DescFieldComp(spec[0]);
      }
    }

    if(comps.length > 1) {
      return new MultiComp(comps);
    } else {
      return comps[0];
    }
  }

  private void constructStreams() throws IOException {

    try {
      zkStateReader = connect();
      ClusterState clusterState = zkStateReader.getClusterState();
      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);
      long time = System.currentTimeMillis();
      params.put("distrib","false"); // We are the aggregator.
      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList();
        for(Replica replica : replicas) {
          shuffler.add(replica);
        }

        Collections.shuffle(shuffler, new Random(time));
        Replica rep = shuffler.get(0);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        SolrStream solrStream = new SolrStream(url, params, partitionKeys);
        solrStream.setWorkers(this.numWorkers, this.workerID);
        solrStream.setFieldMappings(this.fieldMappings);
        solrStreams.add(solrStream);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public ZkStateReader connect() {
    ZkStateReader zkStateReader = null;
    synchronized (this) {

      ZkStateReader zk = null;
      try {
        zk = new ZkStateReader(zkHost, zkClientTimeout, zkConnectTimeout);
        zk.createClusterStateWatchersAndUpdate();
        zkStateReader = zk;
      } catch (InterruptedException e) {
        if (zk != null) zk.close();
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (KeeperException e) {
        if (zk != null) zk.close();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (IOException e) {
        if (zk != null) zk.close();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (TimeoutException e) {
        if (zk != null) zk.close();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (Exception e) {
        if (zk != null) zk.close();
        // do not wrap because clients may be relying on the underlying exception being thrown
        throw e;
      }
    }

    return zkStateReader;
  }


  private void openStreams() throws IOException {
    ExecutorService service = Executors.newCachedThreadPool();
    List<Future<TupleWrapper>> futures = new ArrayList();
    for(TupleStream solrStream : solrStreams) {
      StreamOpener so = new StreamOpener((SolrStream)solrStream, comp);
      Future<TupleWrapper> future =  service.submit(so);
      futures.add(future);
    }

    try {
      for(Future<TupleWrapper> f : futures) {
        TupleWrapper w = f.get();
        if(w != null) {
          tuples.add(w);
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    service.shutdown();
  }

  public void close() throws IOException {
    for(TupleStream solrStream : solrStreams) {
      solrStream.close();
    }
  }

  public Tuple read() throws IOException {
    TupleWrapper tw = tuples.pollFirst();
    if(tw != null) {
      Tuple t = tw.getTuple();
      if(tw.next()) {
        tuples.add(tw);
      }
      return t;
    } else {
      return new Tuple(true);
    }
  }

  protected class TupleWrapper implements Comparable<TupleWrapper> {
    private Tuple tuple;
    private TupleStream stream;
    private Comparator comp;

    public TupleWrapper(TupleStream stream, Comparator comp) {
      this.stream = stream;
      this.comp = comp;
    }

    public int compareTo(TupleWrapper w) {
      if(this == w) {
        return 0;
      }

      int i = comp.compare(tuple, w.tuple);
      if(i == 0) {
        return 1;
      } else {
        return i;
      }
    }

    public boolean equals(Object o) {
      return this == o;
    }

    public Tuple getTuple() {
      return tuple;
    }

    public boolean next() throws IOException {
      this.tuple = stream.read();
      return !tuple.EOF;
    }
  }

  protected class StreamOpener implements Callable<TupleWrapper> {

    private SolrStream stream;
    private Comparator<Tuple> comp;

    public StreamOpener(SolrStream stream, Comparator<Tuple> comp) {
      this.stream = stream;
      this.comp = comp;
    }

    public TupleWrapper call() throws Exception {
      stream.open();
      TupleWrapper wrapper = new TupleWrapper(stream, comp);
      if(wrapper.next()) {
        return wrapper;
      } else {
        return null;
      }
    }
  }
}