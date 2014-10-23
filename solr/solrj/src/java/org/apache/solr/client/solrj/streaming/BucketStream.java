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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/*
String[] buckets = {"a","b"};
Metric
BucketStream bucketStream = new BucketStream(stream,buckets,metrics,"my-metrics","name");

bucketStream.get(
*/

public abstract class BucketStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private long lcount;
  private double dcount;
  private Bucket[] buckets;
  private Metric[] metrics;
  private String outKey;
  private Map<HashKey, Metric[]> bucketMap;
  private static final HashKey metricsKey = new HashKey("metrics");

  public BucketStream(TupleStream tupleStream,
                      Bucket[] buckets,
                      Metric[] metrics,
                      String outKey) {
    this.tupleStream = tupleStream;
    this.buckets = buckets;
    this.metrics = metrics;
    this.outKey = outKey;
    this.bucketMap = new HashMap();
  }

  public BucketStream(TupleStream tupleStream,
                      Metric[] metrics,
                      String outKey) {
    this.tupleStream = tupleStream;
    this.metrics = metrics;
    this.outKey = outKey;
    this.bucketMap = new HashMap();
  }

  public String getOutKey() {
    return this.outKey;
  }

  public Map<HashKey, Metric[]> getBucketMap() {
    return bucketMap;
  }

  Map<HashKey, Metric[]> merge(List<Map<String, Map<String, Double>>> bucketMaps) {

    Map<HashKey, Metric[]> bucketAccumulator = new HashMap();

    for(Map<String, Map<String, Double>> bucketMap : bucketMaps) {
      Iterator it = bucketMap.entrySet().iterator();
      //Iterate the buckets
      while(it.hasNext()) {
        Map.Entry entry = (Map.Entry)it.next();
        String bucketKey = (String)entry.getKey();
        HashKey hashKey = new HashKey(bucketKey);
        List<Map<String, Double>> metricValues = (List<Map<String, Double>>)entry.getValue();

        if(bucketAccumulator.containsKey(hashKey)) {
          Metric[] mergeMetrics = bucketAccumulator.get(bucketKey);
          for(int i=0; i<mergeMetrics.length; i++) {
            mergeMetrics[i].update(metricValues.get(i));
          }
        } else {
          Metric[] mergedMetrics = new Metric[metrics.length];
          for(int i=0; i<metrics.length; i++) {
            mergedMetrics[i] = metrics[i].newInstance();
            mergedMetrics[i].update(metricValues.get(i));
          }
          bucketAccumulator.put(hashKey, mergedMetrics);
        }
      }
    }

    return bucketAccumulator;
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
      Map<String,List<Map<String, Double>>> metricValues = new HashMap();
      Iterator<Map.Entry<HashKey,Metric[]>> it = bucketMap.entrySet().iterator();
      while(it.hasNext()) {
        Map.Entry<HashKey, Metric[]> entry = it.next();
        HashKey key = entry.getKey();
        Metric[] values = entry.getValue();
        List<Map<String, Double>> finished = new ArrayList();

        for(Metric m : values) {
          Map<String, Double> finalMetric = m.metricValue();
          finished.add(finalMetric);
        }
        metricValues.put(key.toString(), finished);
      }
      tuple.set(this.outKey, metricValues);
      return tuple;
    }

    HashKey hashKey = null;
    if(buckets != null) {
      String[] bucketValues = new String[buckets.length];
      for(int i=0; i<buckets.length; i++) {
        bucketValues[i] = buckets[i].getBucketValue(tuple);
      }
      hashKey = new HashKey(tuple, bucketValues);
    } else {
      hashKey = metricsKey;
    }

    Metric[] bucketMetrics = bucketMap.get(hashKey);
    if(bucketMetrics != null) {
      for(Metric bucketMetric : bucketMetrics) {
        bucketMetric.update(tuple);
      }
    } else {
      bucketMetrics = new Metric[metrics.length];
      for(Metric bucketMetric : bucketMetrics) {
        bucketMetric.update(tuple);
      }
      bucketMap.put(hashKey, bucketMetrics);
    }
    return tuple;
  }

  public int getCost() {
    return 0;
  }
}