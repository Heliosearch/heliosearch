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
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class CountStream extends TupleStream implements AggregateStream {

  private TupleStream tupleStream;
  private long lcount;
  private String outKey;

  public CountStream(TupleStream tupleStream, String outKey) {
    this.tupleStream = tupleStream;
    this.outKey = outKey;
  }

  public String getOutKey() {
    return this.outKey;
  }

  public double doubleValue() {
    return (double)lcount;
  }

  public long longValue() {
    return lcount;
  }

  public void mergeAggregates(List values, Map<String, Object> finalAggregates) {
    for(int i=0; i<values.size();i++) {
      Long l = (Long)values.get(i);
      lcount += l.longValue();
    }

    finalAggregates.put(outKey, lcount);
  }

  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {
    Tuple tuple = tupleStream.read();
    if(tuple.EOF) {
      tuple.set(outKey,lcount);
      return tuple;
    }

     ++lcount;

    return tuple;
  }

  public int getCost() {
    return 0;
  }
}