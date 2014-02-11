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
import org.apache.solr.search.mutable.MutableValueLong;

// TODO: somehow unify with int better?
public abstract class LongLeafValues extends LeafValues {
  protected LongFieldStats stats;

  public LongLeafValues(FieldValues fieldValues, LongFieldStats stats) {
    super(fieldValues);
    this.stats = stats;
  }

  @Override
  public LongFieldStats getFieldStats() {
    return stats;
  }

  @Override
  public float floatVal(int doc) {
    return (float) longVal(doc);
  }

  @Override
  public int intVal(int doc) {
    return (int) longVal(doc);
  }

  @Override
  public abstract long longVal(int doc);

  @Override
  public double doubleVal(int doc) {
    return (double) longVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return exists(doc) ? ((LongConverter)fieldValues).longToString( longVal(doc) ) : null;
  }

  @Override
  public Object objectVal(int doc) {
    return exists(doc) ? ((LongConverter)fieldValues).longToObject( longVal(doc) ) : null;
  }

  @Override
  public boolean boolVal(int doc) {
    return longVal(doc) != 0;
  }

  @Override
  public ValueSourceScorer getRangeScorer(AtomicReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper, boolean matchMissing) {
    return getLongRangeScorer((LongConverter)this.fieldValues, this, readerContext, lowerVal, upperVal, includeLower, includeUpper, matchMissing);
  }

  @Override
  public ValueFiller getValueFiller() {
    final LongConverter longConverter = ((LongConverter)this.fieldValues);
    return new ValueFiller() {
      private final MutableValueLong mval = longConverter.newMutableValue();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = longVal(doc);
        mval.exists = mval.value != 0 || exists(doc);
      }
    };
  }

}



class Long64LeafValues extends LongLeafValues {
  private long arr;
  private final BitDocSetNative valid;

  public Long64LeafValues(FieldValues fieldValues, long longPointer, BitDocSetNative valid, LongFieldStats stats) {
    super(fieldValues, stats);
    this.arr = longPointer;
    this.valid = valid;
  }

  @Override
  public long longVal(int doc) {
    return HS.getLong(arr, doc);
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

class Long8LeafValues extends LongLeafValues {
  private long arr;
  private final long valueOffset;
  private final BitDocSetNative valid;

  public Long8LeafValues(FieldValues fieldValues, long bytePointer, long valueOffset, BitDocSetNative valid, LongFieldStats stats) {
    super(fieldValues, stats);
    this.arr = bytePointer;
    this.valueOffset = valueOffset;
    this.valid = valid;
  }

  @Override
  public long longVal(int doc) {
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


class Long16LeafValues extends LongLeafValues {
  private long arr;
  private final long valueOffset;
  private final BitDocSetNative valid;

  public Long16LeafValues(FieldValues fieldValues, long shortPointer, long valueOffset, BitDocSetNative valid, LongFieldStats stats) {
    super(fieldValues, stats);
    this.arr = shortPointer;
    this.valueOffset = valueOffset;
    this.valid = valid;
  }

  @Override
  public long longVal(int doc) {
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


class Long32LeafValues extends LongLeafValues {
  private long arr;
  private final long valueOffset;
  private final BitDocSetNative valid;

  public Long32LeafValues(FieldValues fieldValues, long intPointer, long valueOffset, BitDocSetNative valid, LongFieldStats stats) {
    super(fieldValues, stats);
    this.arr = intPointer;
    this.valueOffset = valueOffset;
    this.valid = valid;
  }

  @Override
  public long longVal(int doc) {
    return valueOffset + HS.getInt(arr, doc);
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


class Long0LeafValues extends LongLeafValues {
  private static LongFieldStats noStats = new LongFieldStats();

  public Long0LeafValues(FieldValues fieldValues) {
    super(fieldValues, noStats);
  }

  @Override
  public long longVal(int doc) {
    return 0L;
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