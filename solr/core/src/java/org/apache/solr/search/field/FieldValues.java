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
import org.apache.lucene.search.SortField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.StrFieldSource;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.valuesource.DoubleFieldSource;
import org.apache.solr.search.function.valuesource.FloatFieldSource;
import org.apache.solr.search.function.valuesource.IntFieldSource;
import org.apache.solr.search.function.valuesource.LongFieldSource;

import java.io.IOException;

public abstract class FieldValues extends ValueSource {
  protected SchemaField field;

  public FieldValues(SchemaField field, QParser qparser) {
    this.field = field;
  }

  public SchemaField getField() {
    return field;
  }

  public String getFieldName() { return field.getName(); }

  public TopValues getTopValues(QueryContext context) {
    assert context != null;

    // TODO: return null for no values indexed???
    // prevent memory filling up by misspelled fields?

    String key = field.getName();

    if (this instanceof StrFieldValues) {
      // string values are special... they can be requested on non-string fields
      FieldType ft = field.getType();
      if (!(ft instanceof StrField || ft instanceof TextField)) {
        key = key + "__FORCED_STRING__";
      }
    }

    // check context cache first
    TopValues entry = context.getTopValues(key);
    if (entry != null && accept(entry)) {
      return entry;
    }

    SolrCache<String, TopValues> fieldCache = context.searcher().getnCache();

    // just use the name for now
    entry = fieldCache.get(key);

    if (entry != null) {
      // if the entry is not accepted, pretend we didn't find it
      if (!(accept(entry))) {
        entry.decref();
        entry = null;
      }
    }

    if (entry == null) {
      TopValues newEntry = createTopValues(context.searcher());
      // is it kosher to synchronize on the cache?
      synchronized (fieldCache) {
        // try again to see if someone beat us to it
        entry = fieldCache.check(key);

        if (entry != null) {
          // if the entry is not accepted, pretend we didn't find it
          if (!(accept(entry))) {
            entry.decref();
            entry = null;
          }
        }

        if (entry == null) {
          newEntry.incref();  // additional reference for the cache
          fieldCache.put(key, newEntry);
          entry = newEntry;
        } else {
          // someone else beat us to it... discard ours.
          newEntry.decref();
        }
      }
    }

    context.setTopValues(key, entry);

    entry.uses.incrementAndGet(); // only increment if it wasn't in our context
    return entry;
  }

  public boolean accept(TopValues values) {
    return true;
  }

  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {

    if (context.searcher()==null || context.searcher().getnCache() == null) {
      // backup for delete-by-query or realtime searchers
      if (field.getType() instanceof TrieIntField) {
        return new IntFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER).getValues(context, readerContext);
      } else if (field.getType() instanceof TrieLongField) {
        return new LongFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_LONG_PARSER).getValues(context, readerContext);
      } else if (field.getType() instanceof TrieFloatField) {
        return new FloatFieldSource( field.getName(), FieldCache.NUMERIC_UTILS_FLOAT_PARSER ).getValues(context, readerContext);
      } else if (field.getType() instanceof TrieDoubleField) {
        return new DoubleFieldSource( field.getName(), FieldCache.NUMERIC_UTILS_DOUBLE_PARSER ).getValues(context, readerContext);
      } else {
        return new StrFieldSource(field.getName()).getValues(context, readerContext);
      }
    }


    TopValues topValues = getTopValues(context);
    return topValues.getLeafValues(context, readerContext);
  }

  @Override
  public boolean equals(Object o) {
    return false;
  }

  @Override
  public int hashCode() {
    return field.hashCode() + getClass().hashCode();
  }

  @Override
  public String description() {
    return "field(" + getFieldName() + ")";
  }

  // use for field stats also (i.e. leave uninverted values blank?)


  // a generic cache for other field related things?
  public abstract TopValues createTopValues(SolrIndexSearcher searcher);


  // TODO: add more params to base class getSortField
  @Override
  public SortField getSortField(boolean reverse) {
    return this.getSortField(reverse, field.sortMissingFirst(), field.sortMissingLast(), null);
  }
}


