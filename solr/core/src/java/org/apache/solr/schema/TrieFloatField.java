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

import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.field.FloatFieldValues;
import org.apache.solr.search.field.LongFieldValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.valuesource.FloatFieldSource;
import org.apache.solr.search.function.valuesource.LongFieldSource;

/**
 * A numeric field that can contain single-precision 32-bit IEEE 754 
 * floating point values.
 *
 * <ul>
 *  <li>Min Value Allowed: 1.401298464324817E-45</li>
 *  <li>Max Value Allowed: 3.4028234663852886E38</li>
 * </ul>
 *
 * <b>NOTE:</b> The behavior of this class when given values of 
 * {@link Float#NaN}, {@link Float#NEGATIVE_INFINITY}, or 
 * {@link Float#POSITIVE_INFINITY} is undefined.
 * 
 * @see Float
 * @see <a href="http://java.sun.com/docs/books/jls/third_edition/html/typesValues.html#4.2.3">Java Language Specification, s4.2.3</a>
 */
public class TrieFloatField extends TrieField implements FloatValueFieldType {
  {
    type=TrieTypes.FLOAT;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    // return super.getValueSource(field, qparser);

    field.checkFieldCacheSource(qparser);

    if (field.hasDocValues() || (field.properties & FieldProperties.LUCENE_FIELDCACHE) !=0 ) {
      return new FloatFieldSource( field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER );
    } else {
      return new FloatFieldValues(field, qparser);
    }

  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    // return super.getSortField(field, top);

    field.checkSortability();

    return new FloatFieldValues(field, null).getSortField(top, field.sortMissingFirst(), field.sortMissingLast(), null);

  }
}
