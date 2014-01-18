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

package org.apache.solr.search.function.valuesource;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.function.FuncValues;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.search.function.funcvalues.FloatFuncValues;
import org.apache.solr.search.mutable.MutableValue;
import org.apache.solr.search.mutable.MutableValueFloat;

import java.io.IOException;
import java.util.Map;

/**
 * <code>QueryValueSource</code> returns the relevance score of the query
 */
public class QueryValueSource extends ValueSource {
  final Query q;
  final float defVal;

  public QueryValueSource(Query q, float defVal) {
    this.q = q;
    this.defVal = defVal;
  }

  public Query getQuery() {
    return q;
  }

  public float getDefaultValue() {
    return defVal;
  }

  @Override
  public String description() {
    return "query(" + q + ",def=" + defVal + ")";
  }

  @Override
  public FuncValues getValues(QueryContext fcontext, AtomicReaderContext readerContext) throws IOException {
    return new QueryFuncValues(this, readerContext, fcontext);
  }

  @Override
  public int hashCode() {
    return q.hashCode() * 29;
  }

  @Override
  public boolean equals(Object o) {
    if (QueryValueSource.class != o.getClass()) return false;
    QueryValueSource other = (QueryValueSource) o;
    return this.q.equals(other.q) && this.defVal == other.defVal;
  }

  @Override
  public void createWeight(QueryContext context, IndexSearcher searcher) throws IOException {
    Weight w = searcher.createNormalizedWeight(q);
    context.put(this, w);
  }
}


class QueryFuncValues extends FloatFuncValues {
  final AtomicReaderContext readerContext;
  final Bits acceptDocs;
  final Weight weight;
  final float defVal;
  final QueryContext fcontext;
  final Query q;

  Scorer scorer;
  int scorerDoc; // the document the scorer is on
  boolean noMatches = false;

  // the last document requested... start off with high value
  // to trigger a scorer reset on first access.
  int lastDocRequested = Integer.MAX_VALUE;


  public QueryFuncValues(QueryValueSource vs, AtomicReaderContext readerContext, QueryContext fcontext) throws IOException {
    super(vs);

    this.readerContext = readerContext;
    this.acceptDocs = readerContext.reader().getLiveDocs();
    this.defVal = vs.defVal;
    this.q = vs.q;
    this.fcontext = fcontext;

    Weight w = fcontext == null ? null : (Weight) fcontext.get(vs);
    if (w == null) {
      IndexSearcher weightSearcher;
      if (fcontext == null) {
        weightSearcher = new IndexSearcher(ReaderUtil.getTopLevelContext(readerContext));
      } else {
        weightSearcher = fcontext.indexSearcher();
        if (weightSearcher == null) {
          weightSearcher = new IndexSearcher(ReaderUtil.getTopLevelContext(readerContext));
        }
      }
      vs.createWeight(fcontext, weightSearcher);
      w = (Weight) fcontext.get(vs);
    }
    weight = w;
  }

  @Override
  public float floatVal(int doc) {
    try {
      if (doc < lastDocRequested) {
        if (noMatches) return defVal;
        scorer = weight.scorer(readerContext, true, false, acceptDocs);
        if (scorer == null) {
          noMatches = true;
          return defVal;
        }
        scorerDoc = -1;
      }
      lastDocRequested = doc;

      if (scorerDoc < doc) {
        scorerDoc = scorer.advance(doc);
      }

      if (scorerDoc > doc) {
        // query doesn't match this document... either because we hit the
        // end, or because the next doc is after this doc.
        return defVal;
      }

      // a match!
      return scorer.score();
    } catch (IOException e) {
      throw new RuntimeException("caught exception in QueryDocVals(" + q + ") doc=" + doc, e);
    }
  }

  @Override
  public boolean exists(int doc) {
    try {
      if (doc < lastDocRequested) {
        if (noMatches) return false;
        scorer = weight.scorer(readerContext, true, false, acceptDocs);
        scorerDoc = -1;
        if (scorer == null) {
          noMatches = true;
          return false;
        }
      }
      lastDocRequested = doc;

      if (scorerDoc < doc) {
        scorerDoc = scorer.advance(doc);
      }

      if (scorerDoc > doc) {
        // query doesn't match this document... either because we hit the
        // end, or because the next doc is after this doc.
        return false;
      }

      // a match!
      return true;
    } catch (IOException e) {
      throw new RuntimeException("caught exception in QueryDocVals(" + q + ") doc=" + doc, e);
    }
  }

  @Override
  public Object objectVal(int doc) {
    try {
      return exists(doc) ? scorer.score() : null;
    } catch (IOException e) {
      throw new RuntimeException("caught exception in QueryDocVals(" + q + ") doc=" + doc, e);
    }
  }

  @Override
  public ValueFiller getValueFiller() {
    //
    // TODO: if we want to support more than one value-filler or a value-filler in conjunction with
    // the FuncValues, then members like "scorer" should be per ValueFiller instance.
    // Or we can say that the user should just instantiate multiple FuncValues.
    //
    return new ValueFiller() {
      private final MutableValueFloat mval = new MutableValueFloat();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) {
        try {
          if (noMatches) {
            mval.value = defVal;
            mval.exists = false;
            return;
          }
          scorer = weight.scorer(readerContext, true, false, acceptDocs);
          scorerDoc = -1;
          if (scorer == null) {
            noMatches = true;
            mval.value = defVal;
            mval.exists = false;
            return;
          }
          lastDocRequested = doc;

          if (scorerDoc < doc) {
            scorerDoc = scorer.advance(doc);
          }

          if (scorerDoc > doc) {
            // query doesn't match this document... either because we hit the
            // end, or because the next doc is after this doc.
            mval.value = defVal;
            mval.exists = false;
            return;
          }

          // a match!
          mval.value = scorer.score();
          mval.exists = true;
        } catch (IOException e) {
          throw new RuntimeException("caught exception in QueryDocVals(" + q + ") doc=" + doc, e);
        }
      }
    };
  }

  @Override
  public String toString(int doc) {
    return "query(" + q + ",def=" + defVal + ")=" + floatVal(doc);
  }
}