package org.apache.solr.search.function.valuesource;

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
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.funcvalues.DocTermsIndexFuncValues;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueStr;

import java.io.IOException;
import java.util.Map;

/**
 * An implementation for retrieving {@link org.apache.solr.search.function.FuncValues} instances for string based fields.
 */
public class BytesRefFieldSource extends FieldCacheSource {

  public BytesRefFieldSource(String field) {
    super(field);
  }

  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    final FieldInfo fieldInfo = readerContext.reader().getFieldInfos().fieldInfo(field);
    // To be sorted or not to be sorted, that is the question
    // TODO: do it cleaner?
    if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
      final BinaryDocValues binaryValues = FieldCache.DEFAULT.getTerms(readerContext.reader(), field, true);
      final Bits docsWithField = FieldCache.DEFAULT.getDocsWithField(readerContext.reader(), field);
      return new FuncValues() {

        @Override
        public boolean exists(int doc) {
          return docsWithField.get(doc);
        }

        @Override
        public boolean bytesVal(int doc, BytesRef target) {
          target.copyBytes(binaryValues.get(doc));
          return target.length > 0;
        }

        public String strVal(int doc) {
          final BytesRef bytes = new BytesRef();
          return bytesVal(doc, bytes)
              ? bytes.utf8ToString()
              : null;
        }

        @Override
        public Object objectVal(int doc) {
          return strVal(doc);
        }

        @Override
        public String toString(int doc) {
          return description() + '=' + strVal(doc);
        }

        @Override
        public ValueFiller getValueFiller() {
          return new ValueFiller() {
            private final MutableValueStr mval = new MutableValueStr();

            @Override
            public MutableValue getValue() {
              return mval;
            }

            @Override
            public void fillValue(int doc) {
              mval.exists = docsWithField.get(doc);
              mval.value.length = 0;
              mval.value.copyBytes(binaryValues.get(doc));
            }
          };
        }

      };
    } else {
      return new DocTermsIndexFuncValues(this, readerContext, field) {

        @Override
        protected String toTerm(String readableValue) {
          return readableValue;
        }

        @Override
        public Object objectVal(int doc) {
          return strVal(doc);
        }

        @Override
        public String toString(int doc) {
          return description() + '=' + strVal(doc);
        }
      };
    }
  }
}
