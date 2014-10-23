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

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

public class MeanMetric implements Metric, Serializable {

  private String column;
  private boolean isDouble;
  private double doubleSum;
  private long longSum;
  private long count;

  public MeanMetric(String column, boolean isDouble) {
    this.column = column;
    this.isDouble = isDouble;
  }

  public String getName() {
    return "mean:"+column;
  }

  public void update(Tuple tuple) {
    ++count;
    if(isDouble) {
      Double d = (Double)tuple.get(column);
      doubleSum += d.doubleValue();
    } else {
      Long l = (Long)tuple.get(column);
      longSum += l.doubleValue();
    }
  }

  public Metric newInstance() {
    return new MeanMetric(column, isDouble);
  }

  public Map<String, Double> metricValue() {
    Map m = new HashMap();
    double dcount = (double)count;
    m.put("count", dcount);
    if(isDouble) {
      double ave = doubleSum/dcount;
      m.put("ave",ave);
      m.put("sum",doubleSum);

    } else {
      double ave = longSum/dcount;
      doubleSum = (double)longSum;
      m.put("ave",ave);
      m.put("sum",doubleSum);
    }

    return m;
  }

  public void update(Map<String, Double> metricValue) {
    double dcount = metricValue.get("count");
    count += (long)dcount;
    if(isDouble) {
      double dsum = metricValue.get("sum");
      doubleSum+=dsum;
    } else {
      double dsum = metricValue.get("sum");
      longSum+=(long)dsum;
    }
  }
}