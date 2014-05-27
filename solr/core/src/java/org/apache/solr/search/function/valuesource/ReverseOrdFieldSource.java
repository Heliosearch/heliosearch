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

package org.apache.solr.search.function.valuesource;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.field.FieldUtil;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.funcvalues.IntFuncValues;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueInt;

import java.io.IOException;
import java.util.Map;

/**
 * Obtains the ordinal of the field value from the default Lucene {@link org.apache.lucene.search.FieldCache} using getTermsIndex()
 * and reverses the order.
 * <br>
 * The native lucene index order is used to assign an ordinal value for each field value.
 * <br>Field values (terms) are lexicographically ordered by unicode value, and numbered starting at 0.
 * <br>
 * Example of reverse ordinal (rord):<br>
 * If there were only three field values: "apple","banana","pear"
 * <br>then rord("apple")=2, rord("banana")=1, ord("pear")=0
 * <p/>
 * WARNING: ord() depends on the position in an index and can thus change when other documents are inserted or deleted,
 * or if a MultiSearcher is used.
 * <br>
 */

public class ReverseOrdFieldSource extends ValueSource {
  public final String field;

  public ReverseOrdFieldSource(String field) {
    this.field = field;
  }

  @Override
  public String description() {
    return "rord(" + field + ')';
  }

  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    final int off = readerContext.docBase;
    final SortedDocValues sindex = FieldUtil.getSortedDocValues(context, context.searcher().getSchema().getField(field), null);

    final int lastOrd = sindex.getValueCount() - 1;

    return new IntFuncValues(this) {
      protected String toTerm(String readableValue) {
        return readableValue;
      }

      @Override
      public int intVal(int doc) {
        return lastOrd -sindex.getOrd(doc + off);
      }

      @Override
      public int ordVal(int doc) {
        return lastOrd - sindex.getOrd(doc + off);
      }

      @Override
      public boolean exists(int doc) {
        return sindex.getOrd(doc + off) >= 0;
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueInt mval = new MutableValueInt();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.value = lastOrd - sindex.getOrd(doc);
            mval.exists = mval.value >= 0;
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || (o.getClass() != ReverseOrdFieldSource.class)) return false;
    ReverseOrdFieldSource other = (ReverseOrdFieldSource) o;
    return this.field.equals(other.field);
  }

  private static final int hcode = ReverseOrdFieldSource.class.hashCode();

  @Override
  public int hashCode() {
    return hcode + field.hashCode();
  }

}
