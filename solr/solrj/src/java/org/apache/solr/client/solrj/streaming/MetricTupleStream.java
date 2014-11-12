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
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/*
* This Stream returns a Single Tuple with the Metrics only.
*/

public class MetricTupleStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream stream;
  private Tuple EOF;

  public MetricTupleStream(TupleStream stream) {
    this.stream = stream;
  }


  public void setStreamContext(StreamContext context) {
    this.stream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(stream);
    return l;
  }

  public void open() throws IOException {
    stream.open();
  }

  public void close() throws IOException {
    stream.close();
  }

  public Tuple read() throws IOException {
    if(EOF == null) {
      while(true) {
        Tuple tuple  = stream.read();
        if(tuple.EOF) {
          Map map = new HashMap();
          map.putAll(tuple.fields);
          map.remove("EOF");
          Tuple t = new Tuple(map);
          EOF = tuple;
          return t;
        }
      }
    } else {
      return EOF;
    }
  }

  public int getCost() {
    return 0;
  }
}