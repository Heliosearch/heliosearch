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

    if (field.hasDocValues() || (field.properties & FieldProperties.LUCENE_FIELDCACHE) !=0 ) {
      return new IntFieldSource( field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER );
    } else {
      return new IntFieldValues(field, qparser);
    }
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    // return super.getSortField(field, top);

    field.checkSortability();

    return new IntFieldValues(field, null).getSortField(top, field.sortMissingFirst(), field.sortMissingLast(), null);

  }


}

