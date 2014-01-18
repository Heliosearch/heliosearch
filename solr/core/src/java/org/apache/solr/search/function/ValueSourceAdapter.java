package org.apache.solr.search.function;

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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueDouble;

import java.io.IOException;
import java.util.Collection;


public class ValueSourceAdapter extends ValueSource {
  protected final org.apache.lucene.queries.function.ValueSource luceneSource;

  public ValueSourceAdapter(org.apache.lucene.queries.function.ValueSource luceneSource) {
    this.luceneSource = luceneSource;
  }

  @Override
  public FuncValues getValues(QueryContext context, AtomicReaderContext readerContext) throws IOException {
    org.apache.lucene.queries.function.FunctionValues luceneValues = luceneSource.getValues(null, readerContext);
    return new FunctionValuesAdapter(luceneValues);
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
    return null;
  }
}


class FunctionValuesAdapter extends FuncValues {
  protected final org.apache.lucene.queries.function.FunctionValues luceneValues;


  FunctionValuesAdapter(FunctionValues luceneValues) {
    this.luceneValues = luceneValues;
  }

  @Override
  public float floatVal(int doc) {
    return luceneValues.floatVal(doc);
  }

  @Override
  public int intVal(int doc) {
    return luceneValues.intVal(doc);
  }

  @Override
  public long longVal(int doc) {
    return luceneValues.longVal(doc);
  }

  @Override
  public double doubleVal(int doc) {
    return luceneValues.doubleVal(doc);
  }

  @Override
  public String strVal(int doc) {
    return luceneValues.strVal(doc);
  }

  @Override
  public boolean boolVal(int doc) {
    return luceneValues.boolVal(doc);
  }

  @Override
  public boolean bytesVal(int doc, BytesRef target) {
    return luceneValues.bytesVal(doc, target);
  }

  @Override
  public Object objectVal(int doc) {
    return luceneValues.objectVal(doc);
  }

  @Override
  public boolean exists(int doc) {
    return luceneValues.exists(doc);
  }

  @Override
  public int ordVal(int doc) {
    return luceneValues.ordVal(doc);
  }

  @Override
  public int numOrd() {
    return luceneValues.numOrd();
  }

  @Override
  public ValueFiller getValueFiller() {
    // current other spatial value sources just use double, so implement directly
    // instead of wrapping.
    return new ValueFiller() {
      private final MutableValueDouble mval = new MutableValueDouble();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        mval.value = luceneValues.doubleVal(doc);
        mval.exists = luceneValues.exists(doc);
      }
    };
  }

  @Override
  public void floatVal(int doc, float[] vals) {
    luceneValues.floatVal(doc, vals);
  }

  @Override
  public void intVal(int doc, int[] vals) {
    luceneValues.intVal(doc, vals);
  }

  @Override
  public void longVal(int doc, long[] vals) {
    luceneValues.longVal(doc, vals);
  }

  @Override
  public void doubleVal(int doc, double[] vals) {
    luceneValues.doubleVal(doc, vals);
  }

  @Override
  public void strVal(int doc, String[] vals) {
    luceneValues.strVal(doc, vals);
  }

  @Override
  public Explanation explain(int doc) {
    return luceneValues.explain(doc);
  }

  @Override
  public ValueSourceScorer getScorer(IndexReader reader) {
    return new ValueSourceScorerAdapter(reader, this, luceneValues.getScorer(reader));
  }

  @Override
  public ValueSourceScorer getRangeScorer(IndexReader reader, String lowerVal, String upperVal, boolean includeLower, boolean includeUpper) {
    return new ValueSourceScorerAdapter(reader, this, luceneValues.getRangeScorer(reader, lowerVal, upperVal, includeLower, includeUpper));
  }

  @Override
  public String toString(int doc) {
    return luceneValues.toString(doc);
  }
}


class ValueSourceScorerAdapter extends ValueSourceScorer {
  protected final org.apache.lucene.queries.function.ValueSourceScorer luceneScorer;

  public ValueSourceScorerAdapter(IndexReader reader, FuncValues values, org.apache.lucene.queries.function.ValueSourceScorer luceneScorer) {
    super(reader, values);
    this.luceneScorer = luceneScorer;
  }

  @Override
  public IndexReader getReader() {
    return luceneScorer.getReader();
  }

  @Override
  public void setCheckDeletes(boolean checkDeletes) {
    luceneScorer.setCheckDeletes(checkDeletes);
  }

  @Override
  public boolean matches(int doc) {
    return luceneScorer.matches(doc);
  }

  @Override
  public boolean matchesValue(int doc) {
    return luceneScorer.matchesValue(doc);
  }

  @Override
  public int docID() {
    return luceneScorer.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return luceneScorer.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return luceneScorer.advance(target);
  }

  @Override
  public float score() throws IOException {
    return luceneScorer.score();
  }

  @Override
  public int freq() throws IOException {
    return luceneScorer.freq();
  }

  @Override
  public long cost() {
    return luceneScorer.cost();
  }

  @Override
  public void score(Collector collector) throws IOException {
    luceneScorer.score(collector);
  }

  @Override
  public boolean score(Collector collector, int max, int firstDocID) throws IOException {
    return luceneScorer.score(collector, max, firstDocID);
  }

  @Override
  public Weight getWeight() {
    return luceneScorer.getWeight();
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return luceneScorer.getChildren();
  }

  @Override
  public AttributeSource attributes() {
    return luceneScorer.attributes();
  }
}