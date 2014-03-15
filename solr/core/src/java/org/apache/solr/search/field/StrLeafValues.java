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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.core.HS;
import org.apache.solr.search.function.ValueSourceScorer;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueStr;

import java.io.IOException;


public abstract class StrLeafValues extends LeafValues {
  protected StrFieldStats stats;

  public StrLeafValues(FieldValues fieldValues, StrFieldStats stats) {
    super(fieldValues);
    this.stats = stats;
  }

  @Override
  public StrFieldStats getFieldStats() {
    return stats;
  }

  @Override
  public boolean exists(int doc) {
    return ordVal(doc) >= 0;
  }

  @Override
  public boolean boolVal(int doc) {
    return exists(doc);
  }

  @Override
  public abstract int ordVal(int doc);  // TODO: single-valued fields will never have ords > 2B (because num_ords <= maxDoc)

  public abstract long termToOrd(BytesRef term);

  public abstract void ordToTerm(long ord, BytesRef target);

  // TODO: can all subclasses support this?
  public abstract long ordToTermPointer(long ord);
  public abstract int termPointerToOrd(long termPointer);


  @Override
  public abstract boolean bytesVal(int doc, BytesRef target);

  @Override
  public String strVal(int doc) {
    BytesRef spare = new BytesRef();
    boolean exists = bytesVal(doc, spare);
    if (!exists) return null;  // TODO: return null or empty string?  If this is changed, objectVal will also need to be changed

    CharsRef spareChars = new CharsRef();
    UnicodeUtil.UTF8toUTF16(spare, spareChars);
    return spareChars.toString();
  }

  @Override
  public Object objectVal(int doc) {
    return strVal(doc);  // strVal currently returns null if the value does not exist
  }

  @Override
  public ValueSourceScorer getRangeScorer(AtomicReaderContext readerContext, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper, boolean matchMissing) {
    int lower = matchMissing ? -1 : 0;
    if (lowerVal != null) {
      lower = (int)termToOrd(new BytesRef(lowerVal));
      if (lower < 0) {
        lower = -lower - 1;
      } else if (!includeLower) {
        lower++;
      }
    }

    int upper = Integer.MAX_VALUE;
    if (upperVal != null) {
      upper = (int)termToOrd(new BytesRef(upperVal));
      if (upper < 0) {
        upper = -upper - 2;
      } else if (!includeUpper) {
        upper--;
      }
    }

    final int ll = lower;
    final int uu = upper;

    return new ValueSourceScorer(readerContext, this) {
      @Override
      public boolean matchesValue(int doc) {
        int ord = ordVal(doc);
        return ord >= ll && ord <= uu;
      }
    };
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

}


class StrArrLeafValues extends StrLeafValues {
  private final LongArray ords;  // contains 1-numOrd, with 0 meaning "missing"... subtract 1 to get the "real" ord
  private final LongArray ordToOffset;  // indexed by ord (so 0 is first real value...)
  private final long termBytes; // offset 0 is first real value

  // offset = avg_term_length * ord + adjustment;

  public StrArrLeafValues(FieldValues fieldValues, LongArray ords, LongArray offsets, long termBytes, StrFieldStats stats) {
    super(fieldValues, stats);
    this.ords = ords;
    this.ordToOffset = offsets;
    this.termBytes = termBytes;
  }


  // testing methods, subject to change with implementation
  public LongArray _getDocToOrdArray() { return ords; }
  public LongArray _getOrdToOffsetArray() { return ordToOffset; }
  public long _getTermBytes() {return termBytes; }



  public long ordToTermPointer(long ord) {
    long offset = ordToOffset.getLong((int)ord);
    assert offset >= 0 && offset < HS.arraySizeBytes(termBytes);
    return termBytes + offset;
  }

  public int termPointerToOrd(long termPointer) {
    int termLen = HS.getTermLength(termPointer);
    long termPointerBytes = termPointer + ((termLen <= 0x7f) ? 1 : 2);

    int low = 0;
    int high = (int)(ordToOffset.getSize() - 1);

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midPointer = ordToTermPointer(mid);
      int cmp = HS.compareLengthPrefixBytes(termLen, termPointerBytes, midPointer);
      if (cmp < 0) {
        high = mid - 1;
      } else if (cmp > 0) {
        low = mid + 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }


  @Override
  public long termToOrd(BytesRef key) {
    int low = 0;
    int high = (int)(ordToOffset.getSize() - 1);

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midPointer = ordToTermPointer(mid);
      int cmp = HS.compareLengthPrefixBytes(midPointer, key);
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }


  @Override
  public int ordVal(int doc) {
    return ords.getInt(doc) - 1;
  }

  @Override
  public void ordToTerm(long ord, BytesRef target) {
    long offset = ordToOffset.getLong((int)ord);
    HS.copyLengthPrefixBytes(termBytes, offset, target);
  }

  @Override
  public boolean bytesVal(int doc, BytesRef target) {
    int ord = ordVal(doc);  // ord for single valued field will be limited to an int
    if (ord < 0) {
      target.length = 0;  // TODO should not be needed...
      return false;
    }
    ordToTerm(ord, target);
    return true;
  }

  @Override
  public long getSizeInBytes() {
    return ords.memSize() + ordToOffset.memSize() + HS.arraySizeBytes(termBytes);
  }

  @Override
  protected void free() {
    try {
      HS.freeArray(termBytes);
      ords.close();
      ordToOffset.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}


class Str0Values extends StrLeafValues {
  // offset = avg_term_length * ord + adjustment;

  public Str0Values(FieldValues fieldValues, StrFieldStats stats) {
    super(fieldValues, stats);
  }

  @Override
  public int ordVal(int doc) {
    return -1;
  }

  @Override
  public void ordToTerm(long ord, BytesRef target) {
    // is this defined for "missing"?
  }

  @Override
  public long ordToTermPointer(long ord) {
    return 0;  // should never be called?
  }

  @Override
  public int termPointerToOrd(long termPointer) {
    return -1;
  }

  @Override
  public long termToOrd(BytesRef term) {
    return -1;
  }

  @Override
  public boolean bytesVal(int doc, BytesRef target) {
    target.length = 0;  // TODO: should not be needed
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
