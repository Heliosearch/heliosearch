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

package org.apache.solr.client.solrj.sql;

import org.apache.solr.client.solrj.streaming.StreamContext;
import org.apache.solr.client.solrj.streaming.TupleStream;
import org.apache.solr.client.solrj.streaming.Tuple;

import com.foundationdb.sql.parser.*;

import java.io.Serializable;
import java.util.Properties;
import java.util.List;
import java.io.IOException;

/**
 *  The SQLStream compiles a SQL statement to TupleStream. It implements the TupleStream interface so it can
 *  be treated like any other stream.
 **/

public class SQLStream extends TupleStream implements Serializable {

  private Properties props;
  private TupleStream tupleStream;

  public SQLStream(String sql, Properties props) {
    this.tupleStream = parse(sql, props);
  }

  public List<TupleStream> children() {
    return tupleStream.children();
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public Tuple read() throws IOException {
    return tupleStream.read();
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public void setStreamContext(StreamContext context) {
    tupleStream.setStreamContext(context);
  }

  private TupleStream parse(String sql, Properties props) {
    return null;
  }
}