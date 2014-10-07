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
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import sun.misc.UUEncoder;
import java.util.concurrent.Callable;

public class ParallelStream extends TupleStream {

  private String collection;
  private String zkHost;
  private TupleStream tupleStream;
  private int workers;
  private String[] partitionKeys;
  private Comparator comp;
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  private transient ZkStateReader zkStateReader;
  private String encoded;
  private TreeSet tupleSet;

  public ParallelStream(String zkHost,
                        String collection,
                        TupleStream tupleStream,
                        int workers,
                        String[] partitionKeys,
                        Comparator comp) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.workers = workers;
    this.partitionKeys = partitionKeys;
    this.comp = comp;
    this.tupleStream = tupleStream;
  }

  public List<TupleStream> children() {
    List l = new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bout);
    out.writeObject(tupleStream);
    byte[] bytes = bout.toByteArray();
    UUEncoder uu = new UUEncoder();
    this.encoded = uu.encode(bytes);
    List<WorkerStream> workerStreams = constructStreams();
  }

  private List<WorkerStream> constructStreams() throws IOException {

    try {
      zkStateReader = connect();
      ClusterState clusterState = zkStateReader.getClusterState();
      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);
      long time = System.currentTimeMillis();
      List<Replica> shuffler = new ArrayList();
      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for(Replica replica : replicas) {
          shuffler.add(replica);
        }
      }

      Collections.shuffle(shuffler, new Random(time));
      List<WorkerStream> workerStreams = new ArrayList();
      for(int i=0; i<workers; i++) {
        Replica replica = shuffler.get(i);
        workerStreams.add(new WorkerStream(replica, this.encoded, i));
      }

      return workerStreams;

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


  public void close() throws IOException {

  }

  public Tuple read() {
    return null;
  }

  class WorkerStream extends TupleStream {

    private String serializedTupleStream;
    private int worker;
    private Replica replica;

    public WorkerStream(Replica replica, String serializedTupleStream, int worker) {
      this.replica = replica;
      this.serializedTupleStream = serializedTupleStream;
      this.worker = worker;
    }

    public void open() {

    }

    public void close() {

    }

    public Tuple read() {
      return null;
    }

    public List<TupleStream> children() {
      return new ArrayList();
    }
  }

  private class StreamOpener implements Callable<TupleStream> {
    public TupleStream call() {
      return null;
    }
  }
}
