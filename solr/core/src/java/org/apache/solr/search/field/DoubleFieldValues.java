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

public class DoubleFieldValues extends FieldValues {

  public DoubleFieldValues(SchemaField field, QParser qparser) {
    super(field, qparser);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof DoubleFieldValues && this.field.equals(((DoubleFieldValues)o).field);
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }

  @Override
  public String description() {
    return "double(" + getFieldName() + ')';
  }

  @Override
  public TopValues createTopValues(SolrIndexSearcher searcher) {
    return new DoubleTopValues(this);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public SortField getSortField(final boolean top, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
    return new DoubleSortField(top, sortMissingFirst, sortMissingLast, missVal);
  }

  // TODO: move to ValueSource?
  class DoubleSortField extends SortField {
    public boolean sortMissingFirst;
    public boolean sortMissingLast;

    public DoubleSortField(boolean reverse, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
      super(DoubleFieldValues.this.getField().getName(), Type.REWRITEABLE, reverse);
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
          missingValue = top ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        } else if ( sortMissingFirst ) {
          missingValue = top ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
      }

      if (!(searcher instanceof SolrIndexSearcher) || DoubleFieldValues.this.getField().hasDocValues()) {
        SortField sf = new SortField( DoubleFieldValues.this.getField().getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, getReverse());
        sf.setMissingValue(this.missingValue);
        return sf;
      }

      QueryContext context = QueryContext.newContext(searcher);
      createWeight(context);
      return new SortField(getField(), new DoubleComparatorSource(context, (Double)this.missingValue), getReverse());
    }
  }

  class DoubleComparatorSource extends FieldComparatorSource {
    private final QueryContext context;
    Double missVal;

    public DoubleComparatorSource(QueryContext context, Double missVal) {
      this.context = context;
      this.missVal = missVal;
    }

    @Override
    public FieldComparator<Double> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) throws IOException {
      return new DoubleComparator(context, numHits, missVal);
    }
  }


  /** Parses field's values as int (using {@link
   *  org.apache.lucene.search.FieldCache#getInts} and sorts by ascending value */
  class DoubleComparator extends FieldComparator<Double> {
    private DoubleLeafValues currentReaderValues;
    private DoubleTopValues topValues;
    private final double[] values;
    private double bottom;                           // Value of bottom of queue
    private double topValue;
    private final QueryContext qcontext;
    private final double missingValue;

    DoubleComparator(QueryContext qcontext, int numHits, Double missVal) {
      this.qcontext = qcontext;
      values = new double[numHits];
      this.missingValue = missVal == null ? 0.0f : missVal;
      this.topValues = (DoubleTopValues) getTopValues(qcontext);
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Double.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      double v2 = currentReaderValues.doubleVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):

      // A generic int value source should prob not compare to 0 first...
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      return Double.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      double v2 = currentReaderValues.doubleVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Double> setNextReader(AtomicReaderContext readerContext) throws IOException {
      currentReaderValues = (DoubleLeafValues)topValues.getLeafValues(qcontext, readerContext);
      return this;
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Double value) {
      topValue = value;
    }

    @Override
    public Double value(int slot) {
      // TODO: return null if missing?
      return Double.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      double docValue = currentReaderValues.doubleVal(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docValue == 0 && !currentReaderValues.exists(doc)) {
        docValue = missingValue;
      }
      return Double.compare(topValue, docValue);
    }
  }
}
