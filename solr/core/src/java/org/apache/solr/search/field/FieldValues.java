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
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.valuesource.IntFieldSource;

import java.io.IOException;

public abstract class FieldValues extends ValueSource {
  protected SchemaField field;  // hang on to SchemaField instead?

  public FieldValues(SchemaField field, QParser qparser) {
    this.field = field;
  }

  public SchemaField getField() {
    return field;
  }

  public TopValues getTopValues(QueryContext context) {
    assert context != null;
    if (context == null) {
      System.out.println("ERROR: CONTEXT IS NULL!!!!!"); throw new RuntimeException();  // nocommit
    }

    // TODO: return null for no values indexed???
    // prevent memory filling up by misspelled fields?

    // check context cache first
    TopValues entry = context.getTopValues(this);
    if (entry != null) return entry;


    SolrCache<String, TopValues> fieldCache = context.searcher().getnCache();

    // just use the name for now
    entry = fieldCache.get(field.getName());
    if (entry == null) {
      TopValues newEntry = createTopValues(context.searcher());
      // is it kosher to synchronize on the cache?
      synchronized (fieldCache) {
        // try again to see if someone beat us to it
        entry = fieldCache.check(field.getName());
        if (entry == null) {
          newEntry.incref();  // additional reference for the cache
          fieldCache.put(field.getName(), newEntry);
          entry = newEntry;
        } else {
          // someone else beat us to it... discard ours.
          newEntry.decref();
        }
      }
    }

    context.setTopValues(this, entry);

    return entry;
  }


  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {

    if (context.searcher()==null) {
      // backup for delete-by-query..
      // TODO: prevent this being constructed if in a deleteByQuery context instead?  via parser (getValueSource is passed this)
      // Subclass of SolrQueryParser used for delete-by-query???
      new IntFieldSource(field.getName(), FieldCache.NUMERIC_UTILS_INT_PARSER).getValues(context, readerContext);
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
    return 0;
  }

  @Override
  public String description() {
    return "field(" + field.getName() + ")";
  }

  // use for field stats also (i.e. leave uninverted values blank?)


  // a generic cache for other field related things?
  public abstract TopValues createTopValues(SolrIndexSearcher searcher);

}


