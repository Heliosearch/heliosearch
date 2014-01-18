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

package org.apache.solr.search.function;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.solr.search.CollapsingQParserPlugin.CollapseScore;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.function.funcvalues.FloatFuncValues;

import java.util.Map;
import java.io.IOException;

public class CollapseScoreFunction extends ValueSource {

  public String description() {
    return "CollapseScoreFunction";
  }

  public boolean equals(Object o) {
    if(o instanceof CollapseScoreFunction){
      return true;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return 1213241257;
  }

  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    final CollapseScore cscore = (CollapseScore) context.get("CSCORE");
    return new FloatFuncValues(this) {
      public float floatVal(int doc) {
        return cscore.score;
      }
    };
  }
}