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

public abstract class TupleStream implements Serializable {

  protected int workers;
  protected String[] partitionKeys;

  public TupleStream(int workers, String[] partitionKeys) {
    this.workers = workers;
    this.partitionKeys = partitionKeys;
  }

  public TupleStream() {
    this(-1, null);
  }

  public void setWorker(int worker) {
    for(TupleStream tupleStream : children()) {
      tupleStream.setWorker(worker);
    }
  }

  public abstract List<TupleStream> children();
  public abstract void open() throws IOException;
  public abstract void close() throws IOException;
  public abstract Tuple read() throws IOException;
}