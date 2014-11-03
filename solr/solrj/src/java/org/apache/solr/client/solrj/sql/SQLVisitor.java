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

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.FromBaseTable;
import com.foundationdb.sql.parser.OrderByColumn;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.TableName;
import com.foundationdb.sql.parser.Visitable;
import org.apache.solr.client.solrj.streaming.CloudSolrStream;
import org.apache.solr.client.solrj.streaming.SolrStream;
import org.apache.solr.client.solrj.streaming.TupleStream;
import com.foundationdb.sql.parser.Visitor;
import java.io.Serializable;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * The SQLVisitor visits with the abstract syntax tree created by the Foundationdb SQLParser. As it visits the nodes
 * it compiles a TupleStream
 **/

class SQLVisitor implements Visitor, Serializable {

  private Properties props;
  private TupleStream tupleStream;
  private List<TableDef> tables = new ArrayList();
  private List<SortDef>  sorts = new ArrayList();
  private List<String> fields = new ArrayList();
  private boolean asc;
  private int state = -1;
  private int ORDER_BY = 1;


  public SQLVisitor(Properties props) {
    this.props = props;
  }

  public Visitable visit(Visitable visitable) throws StandardException{

    if(visitable instanceof FromBaseTable) {
      FromBaseTable baseTable = (FromBaseTable)visitable;
      TableName tableName = baseTable.getTableName();
      String table = tableName.getTableName();
      tables.add(new TableDef(table, props));
    } else if(visitable instanceof ResultColumn) {
      ResultColumn col = (ResultColumn)visitable;
      String cname = col.getName();
      fields.add(cname);
    } else if(visitable instanceof OrderByColumn) {
      OrderByColumn orderByColumn = (OrderByColumn)visitable;
      asc = orderByColumn.isAscending();
      this.state = ORDER_BY;
    } else if(visitable instanceof ColumnReference) {
      ColumnReference cref = (ColumnReference)visitable;
      String ocol = cref.getColumnName();
      if(state == ORDER_BY) {
        SortDef sortDef = new SortDef(asc,ocol);
        sorts.add(sortDef);
      }
    }

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
   TableDef tableDef = tables.get(0);

    Map map = new HashMap();
    StringBuilder fieldBuf =  new StringBuilder();
    boolean comma = false;
    for(String field : fields) {
      if(comma) {
        fieldBuf.append(",");
      }

      fieldBuf.append(field);
      comma = true;
    }

    map.put("fl", fieldBuf.toString());

    StringBuilder sortBuf = new StringBuilder();
    comma = false;
    for(SortDef sortDef : sorts) {
      if(comma) {
        sortBuf.append(",");
      }
      sortBuf.append(sortDef.toString());
      comma = true;
    }
    map.put("sort",sortBuf.toString());

    if(tableDef.isCloud()) {
      map.put("q","*:*");
      map.put("qt", tableDef.getHandler());
      tupleStream = new CloudSolrStream(tableDef.getBaseUrl(), tableDef.getTableName(), map);
    } else {
      map.put("q","*:*");
      map.put("qt", tableDef.getHandler());
      tupleStream = new SolrStream(tableDef.getBaseUrl(), map);
    }


    return this.tupleStream;
  }


  private class TableDef {
    private String baseUrl;
    private String handler;
    private String tableName;

    public TableDef(String tableName, Properties props) {
      baseUrl = props.getProperty(tableName+".baseUrl", "http://locathost:8983/solr/"+tableName); //Default to local collection
      handler = props.getProperty(tableName+".handler", "/select");
      this.tableName = tableName;
    }

    public boolean isCloud() {
      return !(baseUrl.indexOf("http") == 0);
    }

    public String getBaseUrl() {
      return baseUrl;
    }

    public String getTableName() {
      return tableName;
    }

    public String getHandler() {
      return handler;
    }
  }

  private class SortDef {
    private String dir;
    private String field;

    public SortDef(boolean asc, String field) {
      if(asc) {
        this.dir = "asc";
      } else {
        this.dir = "desc";
      }

      this.field = field;
    }

    public String toString() {
      return field+" "+dir;
    }
  }
}