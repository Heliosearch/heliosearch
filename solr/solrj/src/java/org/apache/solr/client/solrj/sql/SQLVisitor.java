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
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.CharConstantNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.FromBaseTable;
import com.foundationdb.sql.parser.GroupByColumn;
import com.foundationdb.sql.parser.OrderByColumn;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.TableName;
import com.foundationdb.sql.parser.GroupByList;
import com.foundationdb.sql.parser.Visitable;
import org.apache.solr.client.solrj.streaming.AscMetricComp;
import org.apache.solr.client.solrj.streaming.DescMetricComp;
import org.apache.solr.client.solrj.streaming.Bucket;
import org.apache.solr.client.solrj.streaming.Metric;
import org.apache.solr.client.solrj.streaming.CloudSolrStream;
import org.apache.solr.client.solrj.streaming.MultiComp;
import org.apache.solr.client.solrj.streaming.ParallelStream;
import org.apache.solr.client.solrj.streaming.RollupStream;
import org.apache.solr.client.solrj.streaming.SolrStream;
import org.apache.solr.client.solrj.streaming.SumMetric;
import org.apache.solr.client.solrj.streaming.RankStream;

import org.apache.solr.client.solrj.streaming.TupleStream;
import org.apache.solr.client.solrj.streaming.Tuple;

import com.foundationdb.sql.parser.Visitor;
import com.foundationdb.sql.parser.ValueNode;

import java.io.Serializable;
import java.io.IOException;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;

/**
 * The SQLVisitor visits with the abstract syntax tree created by the Foundationdb SQLParser. As it visits the nodes
 * it compiles a TupleStream
 **/

class SQLVisitor implements Visitor, Serializable {

  private Properties props;
  private TupleStream tupleStream;
  private List<TableDef> tables = new ArrayList();
  private List<OrderByColumn>  sorts = new ArrayList();
  private List<ResultColumn> fields = new ArrayList();
  private List<GroupByColumn> groupBy = new ArrayList();
  private boolean asc;
  private int state = -1;
  private int ORDER_BY = 1;
  private int numWorkers;
  private String workersZkHost;
  private String workersCollection;
  private boolean parallel = false;
  private BinaryRelationalOperatorNode singleWhere;

  public SQLVisitor(Properties props) {
    this.props = props;
    if(props.containsKey("workers.num")) {
      parallel = true;
      this.numWorkers = Integer.parseInt(props.getProperty("workers.num"));
      this.workersZkHost = props.getProperty("workers.zkhost");
      this.workersCollection = props.getProperty("workers.collection");
    }
  }

  public Visitable visit(Visitable visitable) throws StandardException{

    if(visitable instanceof FromBaseTable) {
      FromBaseTable baseTable = (FromBaseTable)visitable;
      TableName tableName = baseTable.getTableName();
      String table = tableName.getTableName();
      tables.add(new TableDef(table, props));
    } else if(visitable instanceof ResultColumn) {
      ResultColumn col = (ResultColumn)visitable;
      fields.add(col);
    } else if(visitable instanceof OrderByColumn) {
      OrderByColumn orderByColumn = (OrderByColumn)visitable;
      sorts.add(orderByColumn);
    } else if(visitable instanceof GroupByColumn) {
      GroupByColumn groupByColumn = (GroupByColumn)visitable;
      groupBy.add(groupByColumn);
    } else if(visitable instanceof BinaryRelationalOperatorNode) {
      this.singleWhere = (BinaryRelationalOperatorNode)visitable;
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

  public TupleStream getTupleStream() throws IOException {

    TableDef tableDef = tables.get(0);

    Map map = new HashMap();
    StringBuilder fieldBuf =  new StringBuilder();
    boolean comma = false;
    for(ResultColumn field : fields) {
      if(comma) {
        fieldBuf.append(",");
      }
      ValueNode vnode = field.getExpression();
      if(vnode instanceof AggregateNode) {
        AggregateNode an = (AggregateNode)vnode;
        ValueNode op = an.getOperand();
        fieldBuf.append(op.getColumnName());
      } else {
        fieldBuf.append(vnode.getColumnName());
      }
      comma = true;
    }


    map.put("fl", fieldBuf.toString());

    if(groupBy.size() == 0) {
      StringBuilder sortBuf = new StringBuilder();
      comma = false;
      for(OrderByColumn sortDef : sorts) {

        if(comma) {
          sortBuf.append(",");
        }

        sortBuf.append(sortDef.getExpression().getColumnName());

        if(sortDef.isAscending()) {
          sortBuf.append(" asc");
        } else {
          sortBuf.append(" desc");
        }

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
    }

    if(groupBy.size() > 0) {
      //Create the buckets
      Bucket[] buckets = new Bucket[groupBy.size()];

      comma = false;
      StringBuilder sortBuf = new StringBuilder();  // We must sort on the group by fields.
      StringBuilder partBuf = new StringBuilder();

      for(int i=0; i<groupBy.size(); i++) {
        GroupByColumn groupByColumn = groupBy.get(i);
        buckets[i] = new Bucket(groupByColumn.getColumnName());
        //Add sort criteria based on the groupby fields
        if(comma) {
          sortBuf.append(",");
          partBuf.append(",");
        }
        sortBuf.append(groupByColumn.getColumnName());
        partBuf.append(groupByColumn.getColumnName());
        sortBuf.append(" asc");
        comma = true;
      }

      map.put("sort", sortBuf.toString());

      if(parallel) {
        map.put("partitionKeys", partBuf.toString());
      }

      if(tableDef.isCloud()) {
        map.put("qt", tableDef.getHandler());
        tupleStream = new CloudSolrStream(tableDef.getBaseUrl(), tableDef.getTableName(), map);
      } else {
        map.put("qt", tableDef.getHandler());
        tupleStream = new SolrStream(tableDef.getBaseUrl(), map);
      }

      if(singleWhere != null) {
        //Assume equals operator for now.
        ValueNode field = singleWhere.getLeftOperand();
        CharConstantNode query  = (CharConstantNode)singleWhere.getRightOperand();
        map.put("q",field.getColumnName()+":"+query.getValue());
      } else {
        map.put("q", "*:*");
      }

      //Create the metrics
      List<Metric> metricList = new ArrayList();
      for(ResultColumn resultColumn : fields) {
        ValueNode vnode = resultColumn.getExpression();
        if(vnode instanceof AggregateNode) {
          AggregateNode agnode = (AggregateNode)vnode;
          String colName = agnode.getOperand().getColumnName();
          boolean isDouble = true;
          if(colName.endsWith("_i") || colName.endsWith("_l")) {
            isDouble = false;
          }

          String function = agnode.getAggregateName();
          if(function.equalsIgnoreCase("sum")) {
            SumMetric metric = new SumMetric(colName, isDouble);
            metricList.add(metric);
          }
        }
      }

      Metric[] metrics = metricList.toArray(new Metric[metricList.size()]);

      tupleStream = new RollupStream(tupleStream, buckets, metrics);

      //Add the rank stream based on order by clause.

      if(sorts.size() > 0) {
        Comparator<Tuple> comp = null;

        if(sorts.size() == 1) {

          OrderByColumn o = sorts.get(0);
          ValueNode vnode = o.getExpression();
          if(vnode instanceof  AggregateNode) {
            AggregateNode aggregateNode = (AggregateNode)vnode;
            for(int i=0; i<metrics.length; i++) {
              Metric m = metrics[i];
              if(equals(m, aggregateNode)) {
                if(o.isAscending()) {
                  comp = new AscMetricComp(i);
                } else {
                  comp = new DescMetricComp(i);
                }
              }
            }
          }

        } else {
          Comparator<Tuple>[] comps = new Comparator[sorts.size()];
          comp = new MultiComp(comps);
          for(int b=0; b<sorts.size(); b++) {
            OrderByColumn o = sorts.get(b);
            ValueNode vnode = o.getExpression();
            if(vnode instanceof  AggregateNode) {
              AggregateNode aggregateNode = (AggregateNode)vnode;
              for(int i=0; i<metrics.length; i++) {
                Metric m = metrics[i];
                if(equals(m, aggregateNode)) {
                  if(o.isAscending()) {
                    comps[b] = new AscMetricComp(i);
                  } else {
                    comps[b] = new DescMetricComp(i);
                  }
                }
              }
            }
          }
        }

        tupleStream = new RankStream(tupleStream, 10, comp);
        if(parallel) {
          System.out.println("######################################:"+workersZkHost);
          tupleStream = new ParallelStream(workersZkHost,workersCollection, tupleStream, numWorkers, comp);
        }
      }
    }

    return this.tupleStream;
  }

  private boolean equals(Metric metric, AggregateNode aggregateNode) {
    String parts[] = metric.getName().split(":");
    String agname = aggregateNode.getAggregateName();
    if(parts[0].equalsIgnoreCase(agname)) {
      if(parts.length == 1) {
        return true;
      } else {

        if(parts[1].equalsIgnoreCase(aggregateNode.getOperand().getColumnName())) {
          return true;
        }
      }
    }

    return false;
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
}