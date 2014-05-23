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

public class FloatFieldValues extends FieldValues {

  public FloatFieldValues(SchemaField field, QParser qparser) {
    super(field, qparser);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FloatFieldValues && this.field.equals(((FloatFieldValues)o).field);
  }

  @Override
  public int hashCode() {
    return field.hashCode();
  }

  @Override
  public String description() {
    return "float(" + getFieldName() + ')';
  }

  @Override
  public TopValues createTopValues(SolrIndexSearcher searcher) {
    return new FloatTopValues(this);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public SortField getSortField(final boolean top, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
    return new FloatSortField(top, sortMissingFirst, sortMissingLast, missVal);
  }

  // TODO: move to ValueSource?
  class FloatSortField extends SortField {
    public boolean sortMissingFirst;
    public boolean sortMissingLast;

    public FloatSortField(boolean reverse, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
      super(FloatFieldValues.this.getField().getName(), Type.REWRITEABLE, reverse);
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
          missingValue = top ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        } else if ( sortMissingFirst ) {
          missingValue = top ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
        }
      }

      if (!(searcher instanceof SolrIndexSearcher) || FloatFieldValues.this.getField().hasDocValues()) {
        SortField sf = new SortField( FloatFieldValues.this.getField().getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER, getReverse());
        sf.setMissingValue(this.missingValue);
        return sf;
      }

      QueryContext context = QueryContext.newContext(searcher);
      createWeight(context);
      return new SortField(getField(), new FloatComparatorSource(context, (Float)this.missingValue), getReverse());
    }
  }

  class FloatComparatorSource extends FieldComparatorSource {
    private final QueryContext context;
    Float missVal;

    public FloatComparatorSource(QueryContext context, Float missVal) {
      this.context = context;
      this.missVal = missVal;
    }

    @Override
    public FieldComparator<Float> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) throws IOException {
      return new FloatComparator(context, numHits, missVal);
    }
  }


  /** Parses field's values as int (using {@link
   *  org.apache.lucene.search.FieldCache#getInts} and sorts by ascending value */
  class FloatComparator extends FieldComparator<Float> {
    private FloatLeafValues currentReaderValues;
    private FloatTopValues topValues;
    private final float[] values;
    private float bottom;                           // Value of bottom of queue
    private float topValue;
    private final QueryContext qcontext;
    private final float missingValue;

    FloatComparator(QueryContext qcontext, int numHits, Float missVal) {
      this.qcontext = qcontext;
      values = new float[numHits];
      this.missingValue = missVal == null ? 0.0f : missVal;
      this.topValues = (FloatTopValues) getTopValues(qcontext);
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      float v2 = currentReaderValues.floatVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):

      // A generic int value source should prob not compare to 0 first...
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      return Float.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      float v2 = currentReaderValues.floatVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Float> setNextReader(AtomicReaderContext readerContext) throws IOException {
      currentReaderValues = (FloatLeafValues)topValues.getLeafValues(qcontext, readerContext);
      return this;
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Float value) {
      topValue = value;
    }

    @Override
    public Float value(int slot) {
      // TODO: return null if missing?
      return Float.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      float docValue = currentReaderValues.floatVal(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docValue == 0 && !currentReaderValues.exists(doc)) {
        docValue = missingValue;
      }
      return Float.compare(topValue, docValue);
    }
  }
}
