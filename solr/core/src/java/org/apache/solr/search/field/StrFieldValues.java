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


import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldProperties;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.Sorting;

import java.io.IOException;

public class StrFieldValues extends FieldValues {
  // hashCode and equals are not supposed to pay attention to cacheTop
  // even if cacheTop == false, values may be returned that are backed by a top-level cache
  protected boolean cacheTop;

  public StrFieldValues(SchemaField field, QParser qparser, boolean cacheTop) {
    super(field, qparser);
    this.cacheTop = cacheTop;
  }

  public StrFieldValues(SchemaField field, QParser qparser) {
    this(field, qparser, (field.getProperties() & FieldProperties.CACHE_TOP) !=0);
  }

  public boolean cacheTop() {
    return this.cacheTop;
  }

  public void setCacheTop(boolean val) {
    this.cacheTop = val;
  }

  @Override
  public boolean accept(TopValues values) {
    if (!(values instanceof StrTopValues)) return false;
    boolean valueCacheTop = ((StrTopValues)values).cacheTop;
    // Only problem is when we are asking for cacheTop==true and valueCacheTop==false
    // return !(cacheTop && !valueCacheTop);
    return !cacheTop || valueCacheTop;
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
    return "str(" + getFieldName()
//        + (cacheTop ? ", cache=top" : "")  // TODO: this is nice info, but it causes QueryParsingTest to fail (bad test...)
        + ')';
  }

  @Override
  public TopValues createTopValues(SolrIndexSearcher searcher) {
    return new StrTopValues(this);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
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
      createWeight(context);
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
      if (cacheTop) {
        return new TopStrComparatorNative(StrFieldValues.this, context, numHits, missingLast);
      } else {
        return new StrComparatorNative(StrFieldValues.this, context, numHits, missingLast);
      }
    }
  }



}


