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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FieldCache;
import org.apache.solr.search.QParser;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.field.LongFieldValues;
import org.apache.solr.search.function.valuesource.LongFieldSource;
import org.apache.solr.search.mutable.MutableValueDate;
import org.apache.solr.search.mutable.MutableValueLong;
import org.apache.solr.update.processor.TimestampUpdateProcessorFactory; //jdoc
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.search.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;

import java.util.List;
import java.util.Map;
import java.util.Date;
import java.io.IOException;

/**
 * <p>
 * An extension of {@link DateField} that supports the same values and 
 * syntax, but indexes the value more efficiently using a numeric 
 * {@link TrieField} under the covers.  See the description of 
 * {@link DateField} for more details of the supported usage.
 * </p>
 * <p>
 * <b>NOTE:</b> Allthough it is possible to configure a <code>TrieDateField</code> 
 * instance with a default value of "<code>NOW</code>" to compute a timestamp 
 * of when the document was indexed, this is not advisable when using SolrCloud 
 * since each replica of the document may compute a slightly different value. 
 * {@link TimestampUpdateProcessorFactory} is recomended instead.
 * </p>
 *
 * @see DateField
 * @see TrieField
 */
public class TrieDateField extends DateField implements DateValueFieldType {

  final TrieField wrappedField = new TrieField() {{
    type = TrieTypes.DATE;
  }};

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    wrappedField.init(schema, args);
  }

  @Override
  public Date toObject(IndexableField f) {
    return (Date) wrappedField.toObject(f);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return wrappedField.toObject(sf, term);
  }

  @Override
  public Object marshalSortValue(Object value) {
    return value;
  }

  @Override
  public Object unmarshalSortValue(Object value) {
    return value;
  }

  /**
   * @return the precisionStep used to index values into the field
   */
  public int getPrecisionStep() {
    return wrappedField.getPrecisionStep();
  }

  @Override
  public NumericType getNumericType() {
    return wrappedField.getNumericType();
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    wrappedField.write(writer, name, f);
  }

  @Override
  public boolean isTokenized() {
    return wrappedField.isTokenized();
  }

  @Override
  public boolean multiValuedFieldCache() {
    return wrappedField.multiValuedFieldCache();
  }

  @Override
  public String storedToReadable(IndexableField f) {
    return wrappedField.storedToReadable(f);
  }

  @Override
  public String readableToIndexed(String val) {  
    return wrappedField.readableToIndexed(val);
  }

  @Override
  public String toInternal(String val) {
    return wrappedField.toInternal(val);
  }

  @Override
  public String toExternal(IndexableField f) {
    return wrappedField.toExternal(f);
  }

  @Override
  public String indexedToReadable(String _indexedForm) {
    return wrappedField.indexedToReadable(_indexedForm);
  }
  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRef charsRef) {
    // TODO: this could be more efficient, but the sortable types should be deprecated instead
    return wrappedField.indexedToReadable(input, charsRef);
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    return wrappedField.storedToIndexed(f);
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    return wrappedField.createField(field, value, boost);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value, float boost) {
    return wrappedField.createFields(field, value, boost);
  }

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    return wrappedField.getRangeQuery(parser, field, min, max, minInclusive, maxInclusive);
  }
  
  @Override
  public Query getRangeQuery(QParser parser, SchemaField sf, Date min, Date max, boolean minInclusive, boolean maxInclusive) {
    return NumericRangeQuery.newLongRange(sf.getName(), wrappedField.precisionStep,
              min == null ? null : min.getTime(),
              max == null ? null : max.getTime(),
              minInclusive, maxInclusive);
  }

  @Override
  public void checkSchemaField(SchemaField field) {
    wrappedField.checkSchemaField(field);
  }



  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    if (field.hasDocValues() || (field.properties & FieldProperties.LUCENE_FIELDCACHE) !=0 ) {
      return new TrieDateFieldSource( field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER );
    } else {
      return new DateFieldValues(field, qparser);
    }

  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    // return super.getSortField(field, top);

    field.checkSortability();

    return new DateFieldValues(field, null).getSortField(top, field.sortMissingFirst(), field.sortMissingLast(), null);

  }


  // TODO: remove extension of DateField...

}


class DateFieldValues extends LongFieldValues {

  public DateFieldValues(SchemaField sf, QParser qParser) {
    super(sf, qParser);
  }

  @Override
  public String description() {
    return "date(" + field + ')';
  }

  //
  // LongConverter methods
  //
  @Override
  public MutableValueLong newMutableValue() {
    return new MutableValueDate();
  }

  @Override
  public Date longToObject(long val) {
    return new Date(val);
  }

  @Override
  public String longToString(long val) {
    return TrieField.dateField.toExternal(longToObject(val));
  }

  @Override
  public long externalToLong(String extVal) {
    return TrieField.dateField.parseMath(null, extVal).getTime();
  }

}
