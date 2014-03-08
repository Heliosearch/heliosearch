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
import org.apache.solr.core.HS;
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
         return new StrComparator(StrFieldValues.this, context, numHits, missingLast);
    }
  }



}


