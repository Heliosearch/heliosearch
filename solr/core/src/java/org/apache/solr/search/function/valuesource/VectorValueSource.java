package org.apache.solr.search.function.valuesource;
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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;

import java.io.IOException;
import java.util.List;


/**
 * Converts individual ValueSource instances to leverage the FuncValues *Val functions that work with multiple values,
 * i.e. {@link org.apache.solr.search.function.FuncValues#doubleVal(int, double[])}
 */
//Not crazy about the name, but...
public class VectorValueSource extends MultiValueSource {
  protected final List<ValueSource> sources;


  public VectorValueSource(List<ValueSource> sources) {
    this.sources = sources;
  }

  public List<ValueSource> getSources() {
    return sources;
  }

  @Override
  public int dimension() {
    return sources.size();
  }

  public String name() {
    return "vector";
  }

  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    int size = sources.size();

    // special-case x,y and lat,lon since it's so common
    if (size == 2) {
      final FuncValues x = sources.get(0).getValues(context, readerContext);
      final FuncValues y = sources.get(1).getValues(context, readerContext);
      return new FuncValues() {

        @Override
        public void intVal(int doc, int[] vals) {
          vals[0] = x.intVal(doc);
          vals[1] = y.intVal(doc);
        }

        @Override
        public void longVal(int doc, long[] vals) {
          vals[0] = x.longVal(doc);
          vals[1] = y.longVal(doc);
        }

        @Override
        public void floatVal(int doc, float[] vals) {
          vals[0] = x.floatVal(doc);
          vals[1] = y.floatVal(doc);
        }

        @Override
        public void doubleVal(int doc, double[] vals) {
          vals[0] = x.doubleVal(doc);
          vals[1] = y.doubleVal(doc);
        }

        @Override
        public void strVal(int doc, String[] vals) {
          vals[0] = x.strVal(doc);
          vals[1] = y.strVal(doc);
        }

        @Override
        public String toString(int doc) {
          return name() + "(" + x.toString(doc) + "," + y.toString(doc) + ")";
        }
      };
    }


    final FuncValues[] valsArr = new FuncValues[size];
    for (int i = 0; i < size; i++) {
      valsArr[i] = sources.get(i).getValues(context, readerContext);
    }

    return new FuncValues() {

      @Override
      public void floatVal(int doc, float[] vals) {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].floatVal(doc);
        }
      }

      @Override
      public void intVal(int doc, int[] vals) {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].intVal(doc);
        }
      }

      @Override
      public void longVal(int doc, long[] vals) {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].longVal(doc);
        }
      }

      @Override
      public void doubleVal(int doc, double[] vals) {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].doubleVal(doc);
        }
      }

      @Override
      public void strVal(int doc, String[] vals) {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].strVal(doc);
        }
      }

      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        boolean firstTime = true;
        for (FuncValues vals : valsArr) {
          if (firstTime) {
            firstTime = false;
          } else {
            sb.append(',');
          }
          sb.append(vals.toString(doc));
        }
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public void createWeight(QueryContext context, IndexSearcher searcher) throws IOException {
    for (ValueSource source : sources)
      source.createWeight(context, searcher);
  }


  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(');
    boolean firstTime = true;
    for (ValueSource source : sources) {
      if (firstTime) {
        firstTime = false;
      } else {
        sb.append(',');
      }
      sb.append(source);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof VectorValueSource)) return false;

    VectorValueSource that = (VectorValueSource) o;

    return sources.equals(that.sources);

  }

  @Override
  public int hashCode() {
    return sources.hashCode();
  }
}
