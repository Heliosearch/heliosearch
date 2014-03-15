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
import org.apache.solr.search.mutable.MutableValueFloat;
import org.apache.solr.search.mutable.MutableValueInt;


public abstract class FloatLeafValues extends LeafValues {
  protected FloatFieldStats stats;

  public FloatLeafValues(FieldValues fieldValues, FloatFieldStats stats) {
    super(fieldValues);
    this.stats = stats;
  }

  @Override
  public FloatFieldStats getFieldStats() {
    return stats;
  }

  @Override
  public abstract float floatVal(int doc);

  @Override
  public int intVal(int doc) {
    return (int) floatVal(doc);
  }

  @Override
  public long longVal(int doc) {
    return (long) floatVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return (double) floatVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Float.toString(floatVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? floatVal(doc) : null;
  }



  @Override
  public ValueSourceScorer getRangeScorer(AtomicReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper, boolean matchMissing) {
    return getFloatRangeScorer(this, readerContext, lowerVal, upperVal, includeLower, includeUpper, matchMissing);
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueFloat mval = new MutableValueFloat();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = floatVal(doc);
        mval.exists = mval.value != 0.0f || exists(doc);
      }
    };
  }

}



class Float32LeafValues extends FloatLeafValues {
  private long arr;
  private final BitDocSetNative valid;

  public Float32LeafValues(FieldValues fieldValues, long longPointer, BitDocSetNative valid, FloatFieldStats stats) {
    super(fieldValues, stats);
    this.arr = longPointer;
    this.valid = valid;
  }

  @Override
  public float floatVal(int doc) {
    return HS.getFloat(arr, doc);
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



class Float0LeafValues extends FloatLeafValues {
  private static FloatFieldStats noStats = new FloatFieldStats();

  public Float0LeafValues(FieldValues fieldValues) {
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
  public float floatVal(int doc) {
    return 0.0f;
  }
}