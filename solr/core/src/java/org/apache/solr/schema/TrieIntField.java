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

package org.apache.solr.schema;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.field.IntFieldValues;
import org.apache.solr.search.field.IntLeafValues;
import org.apache.solr.search.field.IntTopValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.valuesource.IntFieldSource;

import java.io.IOException;

/**
 * A numeric field that can contain 32-bit signed two's complement integer values.
 *
 * <ul>
 *  <li>Min Value Allowed: -2147483648</li>
 *  <li>Max Value Allowed: 2147483647</li>
 * </ul>
 * 
 * @see Integer
 */
public class TrieIntField extends TrieField implements IntValueFieldType {
  {
    type=TrieTypes.INTEGER;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    // return super.getValueSource(field, qparser);
    field.checkFieldCacheSource(qparser);

    if (field.hasDocValues()) {
      return new IntFieldSource( field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER );
    } else {
      return new IntFieldValues(field, qparser);
    }
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    /***
    Object missingValue = null;
    boolean sortMissingLast  = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    if( sortMissingLast ) {
      missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
    else if( sortMissingFirst ) {
      missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    SortField sf = new SortField( field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER, top);
    sf.setMissingValue(missingValue);
    return sf;
     ***/

    return new IntFieldValues(field, null).getSortField(top, field.sortMissingFirst(), field.sortMissingLast(), null);
  }


}

/**************** nocommit
// TODO: extend ValueSource to handle these different types?

class IntSortField extends SortField {
  IntFieldValues source;
  boolean reverse;
  Integer missingValue;
  FieldCache.Parser parser;

  public IntSortField(IntFieldValues source, boolean reverse, Integer missingValue, FieldCache.Parser parser) {
    super(source.description(), SortField.Type.REWRITEABLE, reverse);
    this.parser = parser;
  }

  @Override
  public SortField rewrite(IndexSearcher searcher) throws IOException {
    if (!(searcher instanceof SolrIndexSearcher)) {
      // DBQ
      SortField sf = new SortField( source.getField().getName(), parser, reverse);
      sf.setMissingValue(missingValue);
      return sf;
    }

    QueryContext context = QueryContext.newContext(searcher);
    source.createWeight(context, searcher);
    return new SortField(getField(), new IntComparatorSource(context, source, missingValue), getReverse());
  }
}


class IntComparatorSource extends FieldComparatorSource {
  IntFieldValues source;
  QueryContext context;
  int missingValue;

  public IntComparatorSource(QueryContext context, IntFieldValues source, int missingValue) {
    this.context = context;
    this.source = source;
    this.missingValue = missingValue;
  }

  @Override
  public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
    // TODO: get top values here, or just go for leaf values?
    // Need TopValues for FieldCache based values also (for backup w/ delete-by-query...)
    return new IntComparator(context, source.getTopValues(context), numHits, missingValue);
  }
}


class IntComparator extends FieldComparator {
  private IntLeafValues currentReaderValues;
  private IntTopValues topValues;
  private final int[] values;
  private int bottom;                           // Value of bottom of queue
  private int topValue;
  private final QueryContext qcontext;
  private Bits docsWithField;
  private final int missingValue;

  IntComparator(QueryContext qcontext, IntTopValues topValues, int numHits, int missingValue) {
    this.qcontext = qcontext;
    values = new int[numHits];
    this.missingValue = missingValue;
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
  }

  @Override
  public void setBottom(final int bottom) {
    this.bottom = values[bottom];
  }

  @Override
  public void setTopValue(Object value) {

  }

  @Override
  public void setTopValue(Integer value) {
    topValue = value;
  }

  @Override
  public Integer value(int slot) {
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

**********************************************************/
