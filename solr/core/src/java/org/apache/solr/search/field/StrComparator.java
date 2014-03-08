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
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.core.HS;
import org.apache.solr.search.QueryContext;

import java.io.IOException;

public class StrComparator extends FieldComparator<BytesRef> {
  protected final QueryContext qcontext;
  protected final StrTopValues topValues;
  protected StrLeafValues strValues;

  protected final int[] ords;
  protected final BytesRef[] values;
  protected final int[] readerGen;
  protected int currentReaderGen = -1;
  protected int bottomSlot = -1;  // only populated if the queue is full
  protected int bottomOrd;


  protected boolean bottomSameReader;

  protected BytesRef bottomValue;

  protected BytesRef topValue;
  protected boolean topSameReader;
  protected int topOrd;

  /** -1 if missing values are sorted first, 1 if they are
   *  sorted last */
  protected final int missingSortCmp;

  /** Which ordinal to use for a missing value. */
  protected final int missingOrd;

  /** Creates this, with control over how missing values
   *  are sorted.  Pass sortMissingLast=true to put
   *  missing values at the end. */
  public StrComparator(FieldValues fieldValues, QueryContext qcontext, int numHits, boolean missingLast) {
    this.qcontext = qcontext;
    this.topValues = (StrTopValues) fieldValues.getTopValues(qcontext);

    ords = new int[numHits];
    values = new BytesRef[numHits];
    readerGen = new int[numHits];
    if (missingLast) {
      missingSortCmp = 1;
      missingOrd = (Integer.MAX_VALUE-1);
    } else {
      missingSortCmp = -1;
      missingOrd = -1;
    }
  }

  public StrComparator(StrComparator prev) {
    // order the same as declared
    this.qcontext = prev.qcontext;
    this.topValues = prev.topValues;
    this.strValues = prev.strValues;

    this.ords = prev.ords;
    this.values = prev.values;
    this.readerGen = prev.readerGen;
    this.currentReaderGen = prev.currentReaderGen;
    this.bottomSlot = prev.bottomSlot;
    this.bottomOrd = prev.bottomOrd;
    this.bottomSameReader = prev.bottomSameReader;  // rely on setNextReader code to change this if necessary
    this.bottomValue = prev.bottomValue;
    this.topValue = prev.topValue;
    this.topSameReader = prev.topSameReader;
    this.topOrd = prev.topOrd;
    this.missingSortCmp = prev.missingSortCmp;
    this.missingOrd = prev.missingOrd;
  }

  @Override
  public int compare(int slot1, int slot2) {
    if (readerGen[slot1] == readerGen[slot2]) {
      return ords[slot1] - ords[slot2];
    }

    final BytesRef val1 = values[slot1];
    final BytesRef val2 = values[slot2];
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }

  @Override
  public int compareBottom(int doc) {
    int docOrd = strValues.ordVal(doc);
    if (docOrd < 0) {
      docOrd = missingOrd;
    }
    if (bottomSameReader) {
      // ord is precisely comparable, even in the equal case
      return bottomOrd - docOrd;
    } else if (bottomOrd >= docOrd) {
      // the equals case always means bottom is > doc
      // (because we set bottomOrd to the lower bound in
      // setBottom):
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public void copy(int slot, int doc) {
    int ord = strValues.ordVal(doc);
    if (ord == -1) {
      ord = missingOrd;
      values[slot] = null;
    } else {
      assert ord >= 0;
      if (values[slot] == null) {
        values[slot] = new BytesRef();
      }
      strValues.ordToTerm(ord, values[slot]);
    }
    ords[slot] = ord;
    readerGen[slot] = currentReaderGen;
  }

  @Override
  public FieldComparator<BytesRef> setNextReader(AtomicReaderContext readerContext) throws IOException {
    strValues = (StrLeafValues)topValues.getLeafValues(qcontext, readerContext);
    currentReaderGen++;

    if (topValue != null) {
      // Recompute topOrd/SameReader
      int ord = (int)strValues.termToOrd(topValue);
      if (ord >= 0) {
        topSameReader = true;
        topOrd = ord;
      } else {
        topSameReader = false;
        topOrd = -ord-2;
      }
    } else {
      topOrd = missingOrd;
      topSameReader = true;
    }
    //System.out.println("  setNextReader topOrd=" + topOrd + " topSameReader=" + topSameReader);

    if (bottomSlot != -1) {
      // Recompute bottomOrd/SameReader
      setBottom(bottomSlot);
    }


    if (strValues instanceof StrArrLeafValues) {
      // TODO: specialize base-class for a specific case and reuse???
      LongArray arr = ((StrArrLeafValues)strValues)._getDocToOrdArray();
      if (arr instanceof LongArray8) {
        return new Ord8(this);
      } else if (arr instanceof LongArray16) {
        return new Ord16(this);
      } else if (arr instanceof LongArray32) {
        return new Ord32(this);
      }
    } else if (strValues instanceof Str0Values) {
      return new Ord0(this);
    }

    //return new StrComparator(this);
    // return new AnyOrd(this);
    return this;
  }

  @Override
  public void setBottom(final int bottom) {
    bottomSlot = bottom;

    bottomValue = values[bottomSlot];
    if (currentReaderGen == readerGen[bottomSlot]) {
      bottomOrd = ords[bottomSlot];
      bottomSameReader = true;
    } else {
      if (bottomValue == null) {
        // missingOrd is null for all segments
        assert ords[bottomSlot] == missingOrd;
        bottomOrd = missingOrd;
        bottomSameReader = true;
        readerGen[bottomSlot] = currentReaderGen;
      } else {
        final int ord = (int)strValues.termToOrd(bottomValue);
        if (ord < 0) {
          bottomOrd = -ord - 2;
          bottomSameReader = false;
        } else {
          bottomOrd = ord;
          // exact value match
          bottomSameReader = true;
          readerGen[bottomSlot] = currentReaderGen;
          ords[bottomSlot] = bottomOrd;
        }
      }
    }
  }

  @Override
  public void setTopValue(BytesRef value) {
    // null is fine: it means the last doc of the prior
    // search was missing this value
    topValue = value;
    //System.out.println("setTopValue " + topValue);
  }

  @Override
  public BytesRef value(int slot) {
    return values[slot];
  }

  @Override
  public int compareTop(int doc) {

    int ord = strValues.ordVal(doc);
    if (ord == -1) {
      ord = missingOrd;
    }

    if (topSameReader) {
      // ord is precisely comparable, even in the equal
      // case
      //System.out.println("compareTop doc=" + doc + " ord=" + ord + " ret=" + (topOrd-ord));
      return topOrd - ord;
    } else if (ord <= topOrd) {
      // the equals case always means doc is < value
      // (because we set lastOrd to the lower bound)
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public int compareValues(BytesRef val1, BytesRef val2) {
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }



  public static class Ord0 extends StrComparator {
    public Ord0(StrComparator prev) {
      super(prev);
    }

    @Override
    public int compareBottom(int doc) {
      int docOrd = missingOrd;
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      ords[slot] = missingOrd;
      values[slot] = null;
      readerGen[slot] = currentReaderGen;
    }
  }


  public static class Ord8 extends StrComparator {
    final LongArray8 longArr;
    final long arr;

    public Ord8(StrComparator prev) {
      super(prev);
      longArr = (LongArray8) ((StrArrLeafValues)prev.strValues)._getDocToOrdArray();
      arr = longArr.getNativeArray();
    }

    @Override
    public int compareBottom(int doc) {
      int docOrd = HS.getByte(arr, doc) - 1;
      if (docOrd < 0) {
        docOrd = missingOrd;
      }
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = HS.getByte(arr, doc) - 1;

      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        strValues.ordToTerm(ord, values[slot]);
      }
      ords[slot] = ord;
      readerGen[slot] = currentReaderGen;
    }

  }



  public static class Ord16 extends StrComparator {
    final LongArray16 longArr;
    final long arr;

    public Ord16(StrComparator prev) {
      super(prev);
      longArr = (LongArray16) ((StrArrLeafValues)prev.strValues)._getDocToOrdArray();
      arr = longArr.getNativeArray();
    }

    @Override
    public int compareBottom(int doc) {
      int docOrd = HS.getShort(arr, doc) - 1;
      if (docOrd < 0) {
        docOrd = missingOrd;
      }
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = HS.getShort(arr, doc) - 1;
      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        strValues.ordToTerm(ord, values[slot]);
      }
      ords[slot] = ord;
      readerGen[slot] = currentReaderGen;
    }

  }


  public static class Ord32 extends StrComparator {
    final LongArray32 longArr;
    final long arr;

    public Ord32(StrComparator prev) {
      super(prev);
      longArr = (LongArray32) ((StrArrLeafValues)prev.strValues)._getDocToOrdArray();
      arr = longArr.getNativeArray();
    }

    @Override
    public int compareBottom(int doc) {
      int docOrd = HS.getInt(arr, doc) - 1;
      if (docOrd < 0) {
        docOrd = missingOrd;
      }
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = HS.getInt(arr, doc) - 1;
      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        strValues.ordToTerm(ord, values[slot]);
      }
      ords[slot] = ord;
      readerGen[slot] = currentReaderGen;
    }

  }

  public static class AnyOrd extends StrComparator {

    public AnyOrd(StrComparator prev) {
      super(prev);
    }

    @Override
    public int compareBottom(int doc) {
      int docOrd = strValues.ordVal(doc);
      if (docOrd < 0) {
        docOrd = missingOrd;
      }
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = strValues.ordVal(doc);
      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        strValues.ordToTerm(ord, values[slot]);
      }
      ords[slot] = ord;
      readerGen[slot] = currentReaderGen;
    }

  }

}
