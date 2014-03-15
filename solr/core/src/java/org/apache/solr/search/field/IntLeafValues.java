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
import org.apache.solr.search.mutable.MutableValueInt;


public abstract class IntLeafValues extends LeafValues {
  protected IntFieldStats stats;

  public IntLeafValues(FieldValues fieldValues, IntFieldStats stats) {
    super(fieldValues);
    this.stats = stats;
  }

  @Override
  public IntFieldStats getFieldStats() {
    return stats;
  }

  @Override
  public float floatVal(int doc) {
    return (float) intVal(doc);
  }

  @Override
  public abstract int intVal(int doc);

  @Override
  public long longVal(int doc) {
    return (long) intVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return (double) intVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return Integer.toString(intVal(doc));
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? intVal(doc) : null;
  }



  @Override
  public ValueSourceScorer getRangeScorer(AtomicReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper, boolean matchMissing) {
    return getIntRangeScorer(this, readerContext, lowerVal, upperVal, includeLower, includeUpper, matchMissing);
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
        mval.value = intVal(doc);
        mval.exists = mval.value != 0 || exists(doc);
      }
    };
  }

}


// TODO: make a ValueStats class with things like minValue, maxValue, docsWithValue, etc...

class Int32LeafValues extends IntLeafValues {
  private long arr;
  private final BitDocSetNative valid;

  public Int32LeafValues(FieldValues fieldValues, long intPointer, BitDocSetNative valid, IntFieldStats stats) {
    super(fieldValues, stats);
    this.arr = intPointer;
    this.valid = valid;
  }

  @Override
  public int intVal(int doc) {
    return HS.getInt(arr, doc);
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

// TODO: use these for long field values also?
class Int8LeafValues extends IntLeafValues {
  private long arr;
  private final int valueOffset;
  private final BitDocSetNative valid;

  public Int8LeafValues(FieldValues fieldValues, long bytePointer, int valueOffset, BitDocSetNative valid, IntFieldStats stats) {
    super(fieldValues, stats);
    this.arr = bytePointer;
    this.valueOffset = valueOffset;
    this.valid = valid;
  }

  @Override
  public int intVal(int doc) {
    return valueOffset + HS.getByte(arr, doc);
  }

  @Override
  public boolean exists(int doc) {
    return valid == null || valid.fastGet(doc);
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

class Int16LeafValues extends IntLeafValues {
  private long arr;
  private final int valueOffset;
  private final BitDocSetNative valid;

  public Int16LeafValues(FieldValues fieldValues, long shortPointer, int valueOffset, BitDocSetNative valid, IntFieldStats stats) {
    super(fieldValues, stats);
    this.arr = shortPointer;
    this.valueOffset = valueOffset;
    this.valid = valid;
  }

  @Override
  public int intVal(int doc) {
    return valueOffset + HS.getShort(arr, doc);
  }

  @Override
  public boolean exists(int doc) {
    return valid == null || valid.fastGet(doc);
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


class Int0LeafValues extends IntLeafValues {
  private static IntFieldStats noStats = new IntFieldStats();

  public Int0LeafValues(FieldValues fieldValues) {
    super(fieldValues, noStats);
  }

  @Override
  public int intVal(int doc) {
    return 0;
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
}