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

import org.apache.lucene.index.IndexReader;
import org.apache.solr.core.HS;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.function.ValueSourceScorer;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueInt;

public abstract class IntLeafValues extends LeafValues {
  protected IntFieldStats stats;

  public IntLeafValues(FieldValues fieldValues, IntFieldStats stats) {
    super(fieldValues);
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
  public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    int lower, upper;

    // instead of using separate comparison functions, adjust the endpoints.

    if (lowerVal == null) {
      lower = Integer.MIN_VALUE;
    } else {
      lower = Integer.parseInt(lowerVal);
      if (!includeLower && lower < Integer.MAX_VALUE) lower++;
    }

    if (upperVal == null) {
      upper = Integer.MAX_VALUE;
    } else {
      upper = Integer.parseInt(upperVal);
      if (!includeUpper && upper > Integer.MIN_VALUE) upper--;
    }

    final int ll = lower;
    final int uu = upper;

    return new ValueSourceScorer(reader, this) {
      @Override
      public boolean matchesValue(int doc) {
        int val = intVal(doc);
        // only check for deleted if it's the default value
        // if (val==0 && reader.isDeleted(doc)) return false;
        return val >= ll && val <= uu;
      }
    };
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
  long arr;
  final BitDocSetNative valid;

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
  long arr;
  final int valueOffset;
  final BitDocSetNative valid;

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
  long arr;
  final int valueOffset;
  final BitDocSetNative valid;

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