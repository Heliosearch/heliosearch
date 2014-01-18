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

package org.apache.solr.analytics.util.valuesource;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.funcvalues.StrFuncValues;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueStr;

/**
 * Abstract {@link ValueSource} implementation which wraps one ValueSource
 * and applies an extendible string function to its values.
 */
public abstract class SingleStringFunction extends ValueSource {
  protected final ValueSource source;
  
  public SingleStringFunction(ValueSource source) {
    this.source = source;
  }

  @Override
  public String description() {
    return name()+"("+source.description()+")";
  }

  abstract String name();
  abstract CharSequence func(int doc, FuncValues vals);

  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    final FuncValues vals =  source.getValues(context, readerContext);
    return new StrFuncValues(this) {
      @Override
      public String strVal(int doc) {
        CharSequence cs = func(doc, vals);
        return cs != null ? cs.toString() : null;
      }
      
      @Override
      public boolean bytesVal(int doc, BytesRef bytes) {
        CharSequence cs = func(doc, vals);
        if( cs != null ){
          bytes.copyChars(func(doc,vals));
          return true;
        } else {
          bytes.bytes = BytesRef.EMPTY_BYTES;
          bytes.length = 0;
          bytes.offset = 0;
          return false;
        }
      }

      @Override
      public Object objectVal(int doc) {
        return strVal(doc);
      }
      
      @Override
      public boolean exists(int doc) {
        return vals.exists(doc);
      }

      @Override
      public String toString(int doc) {
        return name() + '(' + strVal(doc) + ')';
      }

      @Override
      public ValueFiller getValueFiller() {
        return new ValueFiller() {
          private final MutableValueStr mval = new MutableValueStr();

          @Override
          public MutableValue getValue() {
            return mval;
          }

          @Override
          public void fillValue(int doc) {
            mval.exists = bytesVal(doc, mval.value);
          }
        };
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    SingleStringFunction other = (SingleStringFunction)o;
    return this.source.equals(other.source);
  }

  @Override
  public int hashCode() {
    return source.hashCode()+name().hashCode();
  }

}
