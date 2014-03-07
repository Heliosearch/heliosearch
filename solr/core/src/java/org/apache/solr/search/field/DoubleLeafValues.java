package org.apache.solr.search.field;

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
import org.apache.solr.core.HS;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.function.ValueSourceScorer;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueDouble;


public abstract class DoubleLeafValues extends LeafValues {
  protected DoubleFieldStats stats;

  public DoubleLeafValues(FieldValues fieldValues, DoubleFieldStats stats) {
    super(fieldValues);
    this.stats = stats;
  }

  @Override
  public DoubleFieldStats getFieldStats() {
    return stats;
  }

  @Override
  public float floatVal(int doc) {
    return (float) doubleVal(doc);
  }

  @Override
  public abstract double doubleVal(int doc);

  @Override
  public int intVal(int doc) {
    return (int) doubleVal(doc);
  }

  @Override
  public long longVal(int doc) {
    return (long) doubleVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Double.toString(doubleVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? doubleVal(doc) : null;
  }



  @Override
  public ValueSourceScorer getRangeScorer(AtomicReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper, boolean matchMissing) {
    return getDoubleRangeScorer(this, readerContext, lowerVal, upperVal, includeLower, includeUpper, matchMissing);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueDouble mval = new MutableValueDouble();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = doubleVal(doc);
        mval.exists = mval.value != 0.0 || exists(doc);
      }
    };
  }

}




class Double64LeafValues extends DoubleLeafValues {
  private long arr;
  private final BitDocSetNative valid;

  public Double64LeafValues(FieldValues fieldValues, long longPointer, BitDocSetNative valid, DoubleFieldStats stats) {
    super(fieldValues, stats);
    this.arr = longPointer;
    this.valid = valid;
  }

  @Override
  public double doubleVal(int doc) {
    return HS.getDouble(arr, doc);
  }

  @Override
  public boolean exists(int doc) {
    return valid==null || valid.fastGet(doc);
  }

  @Override
  public long getSizeInBytes() {
    return HS.arraySizeBytes(arr) + (valid==null ? 0 : valid.memSize());
  }

  @Override
  protected void free() {
    HS.freeArray(arr);
    arr = 0;
    if (valid != null) {
      valid.decref();
    }
  }
}



class Double0LeafValues extends DoubleLeafValues {
  private static DoubleFieldStats noStats = new DoubleFieldStats();

  public Double0LeafValues(FieldValues fieldValues) {
    super(fieldValues, noStats);
  }

  @Override
  public boolean exists(int doc) {
    return false;
  }

  @Override
  public long getSizeInBytes() {
    return 0;
  }

  @Override
  protected void free() {
  }

  @Override
  public double doubleVal(int doc) {
    return 0.0;
  }
}