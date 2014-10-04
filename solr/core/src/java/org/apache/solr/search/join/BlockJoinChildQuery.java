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

package org.apache.solr.search.join;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.Set;

public class BlockJoinChildQuery extends Query {

  private final Query parentList;
  private final Query parentQuery;
  private final boolean doScores;

  public BlockJoinChildQuery(Query parentQuery, Query parentList, boolean doScores) {
    super();
    this.parentQuery = parentQuery;
    this.parentList = parentList;
    this.doScores = doScores;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new ChildBlockJoinWeight((SolrIndexSearcher)searcher);
  }

  private class ChildBlockJoinWeight extends Weight {
    private final SolrIndexSearcher searcher;
    private final Weight parentWeight;
    private BitDocSetNative parentBitSet;

    public ChildBlockJoinWeight(SolrIndexSearcher searcher) throws IOException {
      this.searcher = searcher;
      this.parentWeight = parentQuery.createWeight(searcher);
    }

    @Override
    public Query getQuery() {
      return BlockJoinChildQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return parentWeight.getValueForNormalization() * getBoost() * getBoost();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      parentWeight.normalize(norm, topLevelBoost * getBoost());
    }

    @Override
    public Scorer scorer(AtomicReaderContext readerContext, Bits acceptDocs) throws IOException {
       Scorer parentScorer = parentWeight.scorer(readerContext, null);
      if (parentScorer == null) return null;

      if (parentBitSet == null) {
        // Query q = QueryUtils.makeQueryable(parentList);
        Query q = parentList;
        parentBitSet = searcher.getDocSetBits(q);
        SolrRequestInfo.getRequestInfo().addCloseHook(parentBitSet);  // TODO: a better place to decref this
      }

      // BitSetSlice parentBits = new BitSetSlice(parentBitSet, readerContext.docBase, readerContext.reader().maxDoc());
      BitSetSlice parentBits = new BitSetSlice(parentBitSet, readerContext.docBase, readerContext.reader().maxDoc());

      return new ChildBlockJoinScorer(readerContext, this, parentScorer, parentBits, doScores, acceptDocs);
    }

    @Override
    public Explanation explain(AtomicReaderContext reader, int doc) throws IOException {
      throw new UnsupportedOperationException(BlockJoinChildQuery.this.toString() +" - explain unsupported");
    }

    @Override
    public boolean scoresDocsOutOfOrder() {
      return false;
    }
  }

  static class ChildBlockJoinScorer extends Scorer {
    private final Scorer parentScorer;
    private final BitSetSlice parentBits;
    private final boolean doScores;
    private final Bits acceptDocs;
    private final Bits liveDocs;

    private float parentScore;
    private int parentFreq = 1;

    private int childDoc = -1;
    private int parentDoc;

    public ChildBlockJoinScorer(AtomicReaderContext reader, Weight weight, Scorer parentScorer, BitSetSlice parentBits, boolean doScores, Bits acceptDocs) {
      super(weight);
      this.doScores = doScores;
      this.parentBits = parentBits;
      this.parentScorer = parentScorer;
      this.acceptDocs = acceptDocs;
      this.liveDocs = reader.reader().getLiveDocs();
    }

    @Override
    public int nextDoc() throws IOException {
      nextChildDoc:
      while (true) {
        if (childDoc+1 >= parentDoc) {
          // OK, we are done iterating through all children
          // matching this one parent doc, so we now nextDoc()
          // the parent.  Use a while loop because we may have
          // to skip over some number of parents w/ no
          // children:
          while (true) {
            parentDoc = parentScorer.nextDoc();

            if (parentDoc == 0) {
              // Degenerate but allowed: parent has no children
              // TODO: would be nice to pull initial parent
              // into ctor so we can skip this if... but it's
              // tricky because scorer must return -1 for
              // .doc() on init...
              parentDoc = parentScorer.nextDoc();
            }

            if (parentDoc == NO_MORE_DOCS) {
              childDoc = NO_MORE_DOCS;
              return childDoc;
            }


            if (liveDocs != null && !liveDocs.get(parentDoc)) {
              continue;
            }

            childDoc = 1 + parentBits.prevSetBit(parentDoc-1);

            if (acceptDocs != null && !acceptDocs.get(childDoc)) {
              continue nextChildDoc;
            }

            if (childDoc < parentDoc) {
              if (doScores) {
                parentScore = parentScorer.score();
                parentFreq = parentScorer.freq();
              }
              return childDoc;
            } else {
              // Degenerate but allowed: parent has no children
            }
          }
        } else {
          childDoc++;
          if (acceptDocs != null && !acceptDocs.get(childDoc)) {
            continue;
          }
          return childDoc;
        }
      }
    }

    @Override
    public int docID() {
      return childDoc;
    }

    @Override
    public float score() throws IOException {
      return parentScore;
    }

    @Override
    public int freq() throws IOException {
      return parentFreq;
    }

    @Override
    public int advance(int target) throws IOException {
     if (target == NO_MORE_DOCS || childDoc == NO_MORE_DOCS) {
        return childDoc = parentDoc = NO_MORE_DOCS;
      }

      assert childDoc < target;

      if (target <= parentDoc) {
        childDoc = target-1;
        return nextDoc();
      }

      parentDoc = parentScorer.advance(target);

      if (parentDoc == NO_MORE_DOCS) {
        return childDoc = NO_MORE_DOCS;
      }

      //  assert parentBits.get(parentDoc);  // this is only true if parentBits does not reflect liveDocs

      if (liveDocs!= null && !liveDocs.get(parentDoc)) {
        childDoc = parentDoc-1;
        return nextDoc();
      }

      if (doScores) {
        parentScore = parentScorer.score();
        parentFreq = parentScorer.freq();
      }

      childDoc = parentBits.prevSetBit(parentDoc-1);
      childDoc = Math.max(target-1, childDoc);

      return nextDoc();
    }

    @Override
    public long cost() {
      return parentScorer.cost();
    }
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    parentQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query q = parentQuery.rewrite(reader);
    if (parentQuery == q) return this;

    Query rewritten = new BlockJoinChildQuery(parentQuery, parentList, doScores);
    rewritten.setBoost(getBoost());
    return rewritten;
  }

  @Override
  public String toString(String field) {
    return "{!child of='"+parentList.toString()+"'}" + parentQuery.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BlockJoinChildQuery)) return false;
    BlockJoinChildQuery other = (BlockJoinChildQuery)o;
    return parentQuery.equals(other.parentQuery)
           && parentList.equals(other.parentList)
           && doScores == other.doScores
           && getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    int hash = 0xf1aec4e6;
    hash = hash * 29 + parentQuery.hashCode();
    hash = hash * 29 + parentList.hashCode();
    hash = hash * 29 + Float.floatToRawIntBits(getBoost());
    return hash;
  }
}
