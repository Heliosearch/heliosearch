package org.apache.solr.search.join;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

public class JoinQuery extends Query {
  String fromField;
  String toField;
  String fromIndex;
  Query fromQ;
  long fromCoreOpenTime;

  public JoinQuery(String fromField, String toField, String fromIndex, Query fromQuery) {
    this.fromField = fromField;
    this.toField = toField;
    this.fromIndex = fromIndex;
    this.fromQ = fromQuery;
  }

  public Query getQuery() { return fromQ; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // don't rewrite the fromQuery, that will be handled later if needed.
    return this;
  }

  @Override
  public void extractTerms(Set terms) {
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    JoinWeight jw = new JoinWeight(this, (SolrIndexSearcher)searcher);
    jw.setImplementation();
    return jw;
  }

  static class JoinWeight extends Weight implements Closeable {
    // Things that various join filter implementations share can go here.
    JoinQuery q;
    JoinImpl impl;
    SolrIndexSearcher fromSearcher;
    RefCounted<SolrIndexSearcher> fromRef;  // null if from the same core
    SolrCore fromCore; // null if from the same core
    SolrIndexSearcher toSearcher;
    private float queryNorm;
    private float queryWeight;
    ResponseBuilder rb;
    boolean debug;


    DocSet fromSet;  // the set of docs we're starting from
    int fromSetSize;


    public JoinWeight(JoinQuery q, SolrIndexSearcher searcher) {
      this.q = q;
      this.fromSearcher = searcher;
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        info.addCloseHook(this);
        rb = info.getResponseBuilder();
      }
      debug = rb != null && rb.isDebug();

      if (q.fromIndex == null) {
        this.fromSearcher = searcher;
      } else {
        if (info == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join must have SolrRequestInfo");
        }

        CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
        fromCore = container.getCore(q.fromIndex);

        if (fromCore == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + q.fromIndex);
        }

        if (info.getReq().getCore() == fromCore) {
          // if this is the same core, use the searcher passed in... otherwise we could be warming and
          // get an older searcher from the core.
          fromSearcher = searcher;
        } else {
          // This could block if there is a static warming query with a join in it, and if useColdSearcher is true.
          // Deadlock could result if two cores both had useColdSearcher and had joins that used eachother.
          // This would be very predictable though (should happen every time if misconfigured)
          fromRef = fromCore.getSearcher(false, true, null);

          // be careful not to do anything with this searcher that requires the thread local
          // SolrRequestInfo in a manner that requires the core in the request to match
          fromSearcher = fromRef.get();
        }
      }
      this.toSearcher = searcher;
    }

    @Override
    public void close() throws IOException {
      if (fromSet != null) {
        fromSet.decref();
        fromSet = null;
      }

      if (fromRef != null) {
        fromRef.decref();
        fromRef = null;
      }

      // Make sure this is after other resources from the "from" core, such as fromRef, etc.
      if (fromCore != null) {
        fromCore.close();
        fromCore = null;
      }
    }

    // call this when first scorer is requested, or right after weight creation?
    public void setImplementation() throws IOException {
      // TODO: set new SolrRequestInfo for new/different core?
      fromSet = fromSearcher.getDocSet(q.fromQ);
      fromSetSize = fromSet.size();

      impl = new TermJoinImpl(q, this);
    }


    @Override
    public Query getQuery() {
      return q;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      queryWeight = q.getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      return impl.scorer(context, acceptDocs);
    }


    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, context.reader().getLiveDocs());
      boolean exists = scorer.advance(doc) == doc;

      ComplexExplanation result = new ComplexExplanation();

      if (exists) {
        result.setDescription(this.toString()
        + " , product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(q.getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm,"queryNorm"));
      } else {
        result.setDescription(this.toString()
        + " doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }


  }


  abstract static class JoinImpl implements Closeable {
    protected final JoinQuery q;
    protected final JoinWeight w;

    public JoinImpl(JoinQuery q, JoinWeight weight) {
      this.q = q;
      this.w = weight;
    }

    public void prepare() {  // experimental
    }

    public void debug() {  // experimental
    }

    public abstract Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException;

    @Override
    public void close() throws IOException {
    }
  }



  @Override
  public String toString(String field) {
    return "{!join from="+fromField+" to="+toField
        + (fromIndex != null ? " fromIndex="+fromIndex : "")
        +"}"+ fromQ.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    JoinQuery other = (JoinQuery)o;
    return this.fromField.equals(other.fromField)
           && this.toField.equals(other.toField)
           && this.getBoost() == other.getBoost()
           && this.fromQ.equals(other.fromQ)
           && (this.fromIndex == other.fromIndex || this.fromIndex != null && this.fromIndex.equals(other.fromIndex))
           && this.fromCoreOpenTime == other.fromCoreOpenTime
        ;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = h * 31 + fromQ.hashCode();
    h = h * 31 + (int)fromCoreOpenTime;
    h = h * 31 + fromField.hashCode();
    h = h * 31 + toField.hashCode();
    return h;
  }

}
