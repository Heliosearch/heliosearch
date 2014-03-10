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
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;

public class IntFieldValues extends FieldValues {

  public IntFieldValues(SchemaField field, QParser qparser) {
    super(field, qparser);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof IntFieldValues && this.field.equals(((IntFieldValues)o).field);
  }

  @Override
  public int hashCode() {
    return field.hashCode() + 0xc729ac54;
  }

  @Override
  public String description() {
    return "int(" + getFieldName() + ')';
  }

  @Override
  public TopValues createTopValues(SolrIndexSearcher searcher) {
    return new IntTopValues(this);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  // @Override TODO
  public SortField getSortField(final boolean top, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
    return new IntSortField(top, sortMissingFirst, sortMissingLast, missVal);
  }

  // TODO: move to ValueSource?
  class IntSortField extends SortField {
    public boolean sortMissingFirst;
    public boolean sortMissingLast;

    public IntSortField(boolean reverse, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
      super(IntFieldValues.this.getField().getName(), SortField.Type.REWRITEABLE, reverse);
      // distrib cursor paging expects the name to match...
      this.sortMissingFirst = sortMissingFirst;
      this.sortMissingLast = sortMissingLast;
      this.missingValue = missVal;  // missingValue is SortField member
    }

    @Override
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      if (missingValue == null) {
        boolean top = getReverse();
        if ( sortMissingLast ) {
          missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        } else if ( sortMissingFirst ) {
          missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }
      }

      if (!(searcher instanceof SolrIndexSearcher) || IntFieldValues.this.getField().hasDocValues()) {
        SortField sf = new SortField( IntFieldValues.this.getField().getName(), FieldCache.NUMERIC_UTILS_INT_PARSER, getReverse());
        sf.setMissingValue(this.missingValue);
        return sf;
      }

      QueryContext context = QueryContext.newContext(searcher);
      createWeight(context);
      return new SortField(getField(), new IntComparatorSource(context, (Integer)this.missingValue), getReverse());
    }
  }

  class IntComparatorSource extends FieldComparatorSource {
    private final QueryContext context;
    Integer missVal;

    public IntComparatorSource(QueryContext context, Integer missVal) {
      this.context = context;
      this.missVal = missVal;
    }

    @Override
    public FieldComparator<Integer> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) throws IOException {
      return new IntComparator(context, numHits, missVal);
    }
  }


  /** Parses field's values as int (using {@link
   *  FieldCache#getInts} and sorts by ascending value */
  class IntComparator extends FieldComparator<Integer> {
    private IntLeafValues currentReaderValues;
    private IntTopValues topValues;
    private final int[] values;
    private int bottom;                           // Value of bottom of queue
    private int topValue;
    private final QueryContext qcontext;
    private final int missingValue;

    IntComparator(QueryContext qcontext, int numHits, Integer missVal) {
      this.qcontext = qcontext;
      values = new int[numHits];
      this.missingValue = missVal == null ? 0 : missVal;
      this.topValues = (IntTopValues) getTopValues(qcontext);
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Integer.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      int v2 = currentReaderValues.intVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):

      // A generic int value source should prob not compare to 0 first...
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      return Integer.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      int v2 = currentReaderValues.intVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Integer> setNextReader(AtomicReaderContext readerContext) throws IOException {
      currentReaderValues = (IntLeafValues)topValues.getLeafValues(qcontext, readerContext);
      return this;
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Integer value) {
      topValue = value;
    }

    @Override
    public Integer value(int slot) {
      // TODO: return null if missing?
      return Integer.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      int docValue = currentReaderValues.intVal(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docValue == 0 && !currentReaderValues.exists(doc)) {
        docValue = missingValue;
      }
      return Integer.compare(topValue, docValue);
    }
  }
}
