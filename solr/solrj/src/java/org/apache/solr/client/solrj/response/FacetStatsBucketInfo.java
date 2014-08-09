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
package org.apache.solr.client.solrj.response;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * Holds facet stats info
 * 
 * From http://heliosearch.org/solr-facet-functions/ sum sum(sales) summation of numeric values avg avg(popularity) average of numeric values sumsq sumsq(rent) sum of squares min min(salary) minimum value max max(mul(price,popularity)) maximum value unique unique(state) number of unique values count count() number of values (documents) in bucket
 * 
 */
public class FacetStatsBucketInfo implements Serializable {
        String fieldName;
        
        String value;

        Object sum;
        Object avg;
        Double sumOfSquares;
        Object min;
        Object max;

        Number count;

        @SuppressWarnings("unchecked")
        public FacetStatsBucketInfo(final NamedList<Object> nl) {
                

                for (final Map.Entry<String, Object> entry : nl) {
                        
                        String szKey = entry.getKey();
                        if (szKey.startsWith("sum")) {
                                this.sum = entry.getValue();
                        } else if (szKey.startsWith("avg")) {
                                String szAggFunc = szKey;
                                int pos1 = szAggFunc.indexOf('(');
                                int pos2 = szAggFunc.indexOf(')');
                                if (pos1 != -1 && pos2 != -1){                                  
                                        this.fieldName = szAggFunc.substring(pos1+1, pos2);
                                }
                                this.avg = entry.getValue();
                        } else if (szKey.startsWith("sumOfSquares")) {
                                this.sumOfSquares = (Double) entry.getValue();
                        } else if (szKey.startsWith("min")) {
                                this.min = entry.getValue();
                        } else if (szKey.startsWith("max")) {
                                this.max = entry.getValue();
                        } else if (szKey.startsWith("count")) {
                                this.count = (Integer) entry.getValue();
                        } else if (szKey.equals("val")) {
                                this.value = (String)entry.getValue();
                                
                        } else {
                                throw new RuntimeException("unknown key: " + szKey
                                                + " ["
                                                + entry.getValue()
                                                + "]");
                        }
                }
        }

//      public String getName() {
//              return this.name;
//      }



        public Object getMin() {
                return this.min;
        }


        @Override
        public String toString() {
                return "FacetStatsBucketInfo [" + (fieldName != null ? "fieldName=" + fieldName
                                + ", " : "")
                                + (value != null ? "value=" + value
                                                + ", " : "")
                                + (sum != null ? "sum=" + sum
                                                + ", " : "")
                                + (avg != null ? "avg=" + avg
                                                + ", " : "")
                                + (sumOfSquares != null ? "sumOfSquares=" + sumOfSquares
                                                + ", " : "")
                                + (min != null ? "min=" + min
                                                + ", " : "")
                                + (max != null ? "max=" + max
                                                + ", " : "")
                                + (count != null ? "count=" + count : "")
                                + "]";
        }

        public Object getMax() {
                return this.max;
        }

        public Object getSum() {
                return this.sum;
        }

        public Integer getCount() {
                if (this.count == null)
                        return null;
                return this.count.intValue();
        }

        public Object getAvg() {
                return avg;
        }

        public String getValue() {
                return value;
        }
        

}
