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
import org.apache.solr.search.mutable.MutableValueLong;

import java.io.IOException;

public class LongFieldValues extends FieldValues implements LongConverter {

  public LongFieldValues(SchemaField field, QParser qparser) {
    super(field, qparser);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof LongFieldValues && this.field.equals(((LongFieldValues)o).field);
  }

  @Override
  public int hashCode() {
    return field.hashCode() + 0xfcc8bc1f;
  }

  @Override
  public String description() {
    return "long(" + getFieldName() + ')';
  }


  //
  // LongConverter methods
  //
  @Override
  public MutableValueLong newMutableValue() {
    return new MutableValueLong();
  }

  @Override
  public Object longToObject(long val) {
    return Long.valueOf(val);
  }

  @Override
  public String longToString(long val) {
    return Long.toString(val);
  }

  @Override
  public long externalToLong(String extVal) {
    return Long.parseLong(extVal);
  }




  @Override
  public TopValues createTopValues(SolrIndexSearcher searcher) {
    return new LongTopValues(this);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public SortField getSortField(final boolean top, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
    return new LongSortField(top, sortMissingFirst, sortMissingLast, missVal);
  }

  // TODO: move to ValueSource?
  class LongSortField extends SortField {
    public boolean sortMissingFirst;
    public boolean sortMissingLast;

    public LongSortField(boolean reverse, boolean sortMissingFirst, boolean sortMissingLast, Object missVal) {
      super(LongFieldValues.this.getField().getName(), Type.REWRITEABLE, reverse);
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
          missingValue = top ? Long.MIN_VALUE : Long.MAX_VALUE;
        } else if ( sortMissingFirst ) {
          missingValue = top ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
      }

      if (!(searcher instanceof SolrIndexSearcher) || LongFieldValues.this.getField().hasDocValues()) {
        SortField sf = new SortField( LongFieldValues.this.getField().getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER, getReverse());
        sf.setMissingValue(this.missingValue);
        return sf;
      }

      QueryContext context = QueryContext.newContext(searcher);
      createWeight(context);
      return new SortField(getField(), new LongComparatorSource(context, (Long)this.missingValue), getReverse());
    }
  }

  class LongComparatorSource extends FieldComparatorSource {
    private final QueryContext context;
    Long missVal;

    public LongComparatorSource(QueryContext context, Long missVal) {
      this.context = context;
      this.missVal = missVal;
    }

    @Override
    public FieldComparator<Long> newComparator(String fieldname, int numHits,
                                                 int sortPos, boolean reversed) throws IOException {
      return new LongComparator(context, numHits, missVal);
    }
  }


  class LongComparator extends FieldComparator {
    private LongLeafValues currentReaderValues;
    private LongTopValues topValues;
    private final long[] values;
    private long bottom;                           // Value of bottom of queue
    private long topValue;
    private final QueryContext qcontext;
    private final long missingValue;

    LongComparator(QueryContext qcontext, int numHits, Long missVal) {
      this.qcontext = qcontext;
      values = new long[numHits];
      this.missingValue = missVal == null ? 0 : missVal;
      this.topValues = (LongTopValues) getTopValues(qcontext);
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Long.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      long v2 = currentReaderValues.longVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):

      // A generic int value source should prob not compare to 0 first...
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      return Long.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      long v2 = currentReaderValues.longVal(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (v2 == 0 && !currentReaderValues.exists(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Long> setNextReader(AtomicReaderContext readerContext) throws IOException {
      currentReaderValues = (LongLeafValues)topValues.getLeafValues(qcontext, readerContext);
      return this;
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Object value) {
      topValue = (Long)value;
    }

    @Override
    public Object value(int slot) {
      return new Long( values[slot] );
      // return longToObject(values[slot]);  // fsv=true returns longs for the sort values...
    }

    @Override
    public int compareTop(int doc) {
      long docValue = currentReaderValues.longVal(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docValue == 0 && !currentReaderValues.exists(doc)) {
        docValue = missingValue;
      }
      return Long.compare(topValue, docValue);
    }
  }
}
