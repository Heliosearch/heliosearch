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
import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;

public class ParallelStream extends TupleStream {

  private String collection;
  private String zkHost;
  private TupleStream tupleStream;
  private int workers;
  private String[] partitionKeys;
  private Comparator comp;


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

  }

  public List<TupleStream> children() {
    List l = new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {

  }

  public void close() throws IOException {

  }

  public Tuple read() {
    return null;
  }
}
