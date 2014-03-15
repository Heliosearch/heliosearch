package org.apache.solr.schema;

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

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.function.ValueSourceAdapter;

/**
 * Custom field wrapping an int, to test sorting via a custom comparator.
 */
public class WrappedIntField extends TrieIntField {
  Expression expr;

  public WrappedIntField() {
    try {
      expr = JavascriptCompiler.compile("payload % 3");
    } catch (Exception e) {
      throw new RuntimeException("impossible?", e);
    }
  }

  @Override
  public SortField getSortField(final SchemaField field, final boolean reverse) {
    field.checkSortability();

    // TODO: no support for rewritable sort fields!
    /***
    SimpleBindings bindings = new SimpleBindings();
    bindings.add(super.getSortField(field, reverse));
    SortField ret = expr.getSortField(bindings, reverse);
    ***/

    SimpleBindings bindings = new SimpleBindings();
    bindings.add(new SortField(field.name, FieldCache.NUMERIC_UTILS_INT_PARSER));
    return expr.getSortField(bindings, reverse);
  }

}
