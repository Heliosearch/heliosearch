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


import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;

public class FieldUtil {

  /** The returned StrLeafValues do not need to be reference counted,
   * they will be valid for the lifetime of the context (at a minimum... normally the request context).
   * These returned StrLeafValues are actually top-level (i.e. the "leaf" used is the top-level reader).
   */
  public static StrLeafValues getTopStrings(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    StrFieldValues fieldValues = new StrFieldValues(field, qparser, true);
    TopValues vals = fieldValues.getTopValues(context);
    if (!(vals instanceof StrTopValues)) {
      // This should now be impossible...
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "getTopStrings type mismatch for field " + field + ", found " + vals);
    }
    StrTopValues svals = (StrTopValues) vals;
    assert svals.cacheTop;

    StrLeafValues actualValues = svals.createTopValue(context);
    return actualValues;
  }

  /** Simpler method that creates a request context and looks up the field for you */
  public static SortedDocValues getSortedDocValues(SolrIndexSearcher searcher, String field) throws IOException {
    SchemaField sf = searcher.getSchema().getField(field);
    QueryContext qContext = QueryContext.newContext(searcher);
    return getSortedDocValues( qContext, sf, null );
  }


  public static SortedDocValues getSortedDocValues(QueryContext context, SchemaField field, QParser qparser) throws IOException {
    if (!field.hasDocValues() && (field.getType() instanceof StrField || field.getType() instanceof TextField)) {
      return getSortedDocValues(getTopStrings(context, field, qparser));
    }

    SortedDocValues si = FieldCache.DEFAULT.getTermsIndex(context.searcher().getAtomicReader(), field.getName());
    return si;
  }


  public static SortedDocValues getSortedDocValues(StrLeafValues strVals) {
    return new NativeSortedDocValues(strVals);
  }
}

class NativeSortedDocValues extends SortedDocValues {
  protected final StrLeafValues vals;
  public NativeSortedDocValues(StrLeafValues vals) {
    this.vals = vals;
  }

  public StrLeafValues getWrappedValues() {
    return vals;
  }

  @Override
  public int getOrd(int docID) {
    return vals.ordVal(docID);
  }

  @Override
  public void lookupOrd(int ord, BytesRef result) {
    vals.ordToTerm(ord, result);
  }

  @Override
  public int getValueCount() {
    return (int)vals.getFieldStats().getNumUniqueValues();
  }

  @Override
  public int lookupTerm(BytesRef key) {
    return (int)vals.termToOrd(key);
  }
}
