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
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.Sorting;

import java.io.IOException;

public class StrFieldValues extends FieldValues {

  public StrFieldValues(SchemaField field, QParser qparser) {
    super(field, qparser);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof StrFieldValues && this.field.equals(((StrFieldValues)o).field);
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }

  @Override
  public String description() {
    return "str(" + getFieldName() + ')';
  }

  @Override
  public TopValues createTopValues(SolrIndexSearcher searcher) {
    return new StrTopValues(this);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  // @Override TODO
  public SortField getSortField(final boolean top, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
    return new StrSortField(top, sortMissingFirst, sortMissingLast);
  }

  // TODO: move to ValueSource?
  class StrSortField extends SortField {
    public boolean sortMissingFirst;
    public boolean sortMissingLast;

    public StrSortField(boolean reverse, boolean sortMissingFirst, boolean sortMissingLast) {
      super(StrFieldValues.this.getField().getName(), Type.REWRITEABLE, reverse);
      // distrib cursor paging expects the name to match...
      this.sortMissingFirst = sortMissingFirst;
      this.sortMissingLast = sortMissingLast;
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      boolean missingLast = false;  // sort missing last, taking into account comparator reversal
      if (sortMissingFirst && getReverse()) {
        missingLast = true;
      } else if (sortMissingLast && !getReverse()) {
        missingLast = true;
      }

      if (!(searcher instanceof SolrIndexSearcher) || StrFieldValues.this.getField().hasDocValues()) {
        return Sorting.getStringSortField(getFieldName(), getReverse(), sortMissingLast, sortMissingFirst);  // this method takes sortMissingLast before sortMissingFirst
      }

      QueryContext context = QueryContext.newContext(searcher);
      createWeight(context, searcher);
      return new SortField(getField(), new StrComparatorSource(context, missingLast), getReverse() );
    }
  }

  class StrComparatorSource extends FieldComparatorSource {
    private final QueryContext context;
    boolean missingLast;

    public StrComparatorSource(QueryContext context, boolean missingLast) {
      this.context = context;
      this.missingLast = missingLast;
    }

    @Override
    public FieldComparator<BytesRef> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) throws IOException {
      return new TermOrdValComparator(context, numHits, missingLast);
    }
  }


  public class TermOrdValComparator extends FieldComparator<BytesRef> {
    private final QueryContext qcontext;
    private final StrTopValues topValues;
    private StrLeafValues strValues;

    /* Ords for each slot.
       @lucene.internal */
    final int[] ords;

    /* Values for each slot.
       @lucene.internal */
    final BytesRef[] values;

    /* Which reader last copied a value into the slot. When
       we compare two slots, we just compare-by-ord if the
       readerGen is the same; else we must compare the
       values (slower).
       @lucene.internal */
    final int[] readerGen;

    /* Gen of current reader we are on.
       @lucene.internal */
    int currentReaderGen = -1;

    /* Bottom slot, or -1 if queue isn't full yet
       @lucene.internal */
    int bottomSlot = -1;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot
       is set).  Cached for faster compares.
       @lucene.internal */
    int bottomOrd;

    /* True if current bottom slot matches the current
       reader.
       @lucene.internal */
    boolean bottomSameReader;

    /* Bottom value (same as values[bottomSlot] once
       bottomSlot is set).  Cached for faster compares.
      @lucene.internal */
    BytesRef bottomValue;

    /** Set by setTopValue. */
    BytesRef topValue;
    boolean topSameReader;
    int topOrd;

    private int docBase;

    final BytesRef tempBR = new BytesRef();

    /** -1 if missing values are sorted first, 1 if they are
     *  sorted last */
    final int missingSortCmp;

    /** Which ordinal to use for a missing value. */
    final int missingOrd;

    /** Creates this, with control over how missing values
     *  are sorted.  Pass sortMissingLast=true to put
     *  missing values at the end. */
    public TermOrdValComparator(QueryContext qcontext, int numHits, boolean missingLast) {
      this.qcontext = qcontext;
      this.topValues = (StrTopValues) getTopValues(qcontext);

      ords = new int[numHits];
      values = new BytesRef[numHits];
      readerGen = new int[numHits];
      if (missingLast) {
        missingSortCmp = 1;
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingSortCmp = -1;
        missingOrd = -1;
      }
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
      assert bottomSlot != -1;
      int docOrd = strValues.ordVal(doc);
      if (docOrd == -1) {
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
      docBase = readerContext.docBase;
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
  }

}
