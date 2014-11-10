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

import org.apache.solr.client.solrj.streaming.TupleStream;
import com.foundationdb.sql.parser.StatementNode;
import java.util.Properties;

/**
 *  The SQLStream compiles a SQL statement to TupleStream. It implements the TupleStream interface so it can
 *  be treated like any other stream.
 **/

public class SQLParser {

  public static TupleStream parse(String sql, Properties props) throws Exception {
    SQLVisitor visitor = new SQLVisitor(props);
    com.foundationdb.sql.parser.SQLParser parser = new com.foundationdb.sql.parser.SQLParser();
    StatementNode statementNode = parser.parseStatement(sql);
    statementNode.accept(visitor);
    return visitor.getTupleStream();
  }
}