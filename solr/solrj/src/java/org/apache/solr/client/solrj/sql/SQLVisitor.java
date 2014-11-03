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

import com.foundationdb.sql.parser.Visitable;
import org.apache.solr.client.solrj.streaming.TupleStream;
import com.foundationdb.sql.parser.Visitor;
import java.io.Serializable;
import java.util.Properties;

/**
 * The SQLVisitor visits with the abstract syntax tree created by the Foundationdb SQLParser. As it visits the nodes
 * it compiles a TupleStream
 **/

class SQLVisitor implements Visitor, Serializable {

  private Properties props;
  private TupleStream tupleStream;

  public SQLVisitor(Properties props) {
    this.props = props;
  }

  public Visitable visit(Visitable visitable) {
    return visitable;
  }

  public boolean stopTraversal() {
    return false;
  }

  public boolean visitChildrenFirst(Visitable visitable) {
    return false;
  }

  public boolean skipChildren(Visitable visitable) {
    return false;
  }

  public TupleStream getTupleStream() {
    return this.tupleStream;
  }
}