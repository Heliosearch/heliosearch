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
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

class BlockJoinParentQuery extends Query {
  private final Query parentList;
  private final Query childQuery;

  private final ScoreMode scoreMode;


  public BlockJoinParentQuery(Query childQuery, Query parentList, ScoreMode scoreMode) {
    this.childQuery = childQuery;
    this.parentList = parentList;
    this.scoreMode = scoreMode;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new BlockJoinWeight((SolrIndexSearcher)searcher);
  }

  private class BlockJoinWeight extends Weight {
    private SolrIndexSearcher searcher;
    private Weight childWeight;
    private BitDocSetNative parentBitSet;


    public BlockJoinWeight(SolrIndexSearcher searcher) throws IOException {
      this.searcher = searcher;
      this.childWeight = childQuery.createWeight(searcher);
    }

    @Override
    public Query getQuery() {
      return BlockJoinParentQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return childWeight.getValueForNormalization() * getBoost() * getBoost();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      childWeight.normalize(norm, topLevelBoost * getBoost());
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // parent document space
    @Override
    public Scorer scorer(AtomicReaderContext readerContext, Bits acceptDocs) throws IOException {

      // Pass scoreDocsInOrder true, topScorer false to our sub and the live docs:
      final Scorer childScorer = childWeight.scorer(readerContext, readerContext.reader().getLiveDocs());

      if (childScorer == null) {
        // No matches
        return null;
      }

      final int firstChildDoc = childScorer.nextDoc();
      if (firstChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
        // No matches
        return null;
      }

      if (parentBitSet == null) {
        // Query q = QueryUtils.makeQueryable(parentList);
        Query q = parentList;
        parentBitSet = searcher.getDocSetBits(q);
        SolrRequestInfo.getRequestInfo().addCloseHook(parentBitSet);  // TODO: a better place to decref this
      }

      BitSetSlice parentBits = new BitSetSlice(parentBitSet, readerContext.docBase, readerContext.reader().maxDoc());

      return new BlockJoinScorer(this, childScorer, parentBits, firstChildDoc, scoreMode, acceptDocs);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      BlockJoinScorer scorer = (BlockJoinScorer) scorer(context,  context.reader().getLiveDocs());
      if (scorer != null && scorer.advance(doc) == doc) {
        return scorer.explain(context.docBase);
      }
      return new ComplexExplanation(false, 0.0f, "Not a match");
    }

    @Override
    public boolean scoresDocsOutOfOrder() {
      return false;
    }
  }

  static class BlockJoinScorer extends Scorer {
    private final Scorer childScorer;
    private final BitSetSlice parentBits;
    private final ScoreMode scoreMode;
    private final Bits acceptDocs;
    private int parentDoc = -1;
    private int prevParentDoc;
    private float parentScore;
    private int parentFreq;
    private int nextChildDoc;

    private int[] pendingChildDocs = new int[5];
    private float[] pendingChildScores;
    private int childDocUpto;

    public BlockJoinScorer(Weight weight, Scorer childScorer, BitSetSlice parentBits, int firstChildDoc, ScoreMode scoreMode, Bits acceptDocs) {
      super(weight);
      this.parentBits = parentBits;
      this.childScorer = childScorer;
      this.scoreMode = scoreMode;
      this.acceptDocs = acceptDocs;
      if (scoreMode != ScoreMode.None) {
        pendingChildScores = new float[5];
      }
      nextChildDoc = firstChildDoc;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(childScorer, "BLOCK_JOIN"));
    }

    @Override
    public int nextDoc() throws IOException {
      // Loop until we hit a parentDoc that's accepted
      while (true) {
        if (nextChildDoc == NO_MORE_DOCS) {
          return parentDoc = NO_MORE_DOCS;
        }

        // Gather all children sharing the same parent as
        // nextChildDoc

        parentDoc = parentBits.nextSetBit(nextChildDoc);

        // Parent & child docs are supposed to be
        // orthogonal:
        // TODO: think about relaxing this
        if (nextChildDoc == parentDoc) {
          throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
        }

        assert parentDoc != -1;

        if (acceptDocs != null && !acceptDocs.get(parentDoc)) {
          // Parent doc not accepted; skip child docs until
          // we hit a new parent doc:
          do {
            nextChildDoc = childScorer.nextDoc();
          } while (nextChildDoc < parentDoc);

          // Parent & child docs are supposed to be
          // orthogonal:
          if (nextChildDoc == parentDoc) {
            throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
          }

          continue;
        }

        float totalScore = 0;
        float maxScore = Float.NEGATIVE_INFINITY;

        childDocUpto = 0;
        parentFreq = 0;
        do {

          if (pendingChildDocs.length == childDocUpto) {
            pendingChildDocs = ArrayUtil.grow(pendingChildDocs);
          }
          if (scoreMode != ScoreMode.None && pendingChildScores.length == childDocUpto) {
            pendingChildScores = ArrayUtil.grow(pendingChildScores);
          }
          pendingChildDocs[childDocUpto] = nextChildDoc;
          if (scoreMode != ScoreMode.None) {
            final float childScore = childScorer.score();
            final int childFreq = childScorer.freq();
            pendingChildScores[childDocUpto] = childScore;
            maxScore = Math.max(childScore, maxScore);
            totalScore += childScore;
            parentFreq += childFreq;
          }
          childDocUpto++;
          nextChildDoc = childScorer.nextDoc();
        } while (nextChildDoc < parentDoc);

        // Parent & child docs are supposed to be
        // orthogonal:
        if (nextChildDoc == parentDoc) {
          throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
        }

        switch(scoreMode) {
          case Avg:
            parentScore = totalScore / childDocUpto;
            break;
          case Max:
            parentScore = maxScore;
            break;
          case Total:
            parentScore = totalScore;
            break;
          case None:
            break;
        }

        return parentDoc;
      }
    }

    @Override
    public int docID() {
      return parentDoc;
    }

    @Override
    public float score() throws IOException {
      return parentScore;
    }

    @Override
    public int freq() {
      return parentFreq;
    }

    @Override
    public int advance(int parentTarget) throws IOException {

      if (parentTarget == NO_MORE_DOCS) {
        return parentDoc = NO_MORE_DOCS;
      }

      if (parentTarget == 0) {
        // Callers should only be passing in a docID from
        // the parent space, so this means this parent
        // has no children (it got docID 0), so it cannot
        // possibly match.  We must handle this case
        // separately otherwise we pass invalid -1 to
        // prevSetBit below:
        return nextDoc();
      }

      prevParentDoc = parentBits.prevSetBit(parentTarget-1);

      assert prevParentDoc >= parentDoc;
      if (prevParentDoc > nextChildDoc) {
        nextChildDoc = childScorer.advance(prevParentDoc);
      }

      // Parent & child docs are supposed to be orthogonal:
      if (nextChildDoc == prevParentDoc) {
        throw new IllegalStateException("child query must only match non-parent docs, but parent docID=" + nextChildDoc + " matched childScorer=" + childScorer.getClass());
      }

      return nextDoc();
    }

    public Explanation explain(int docBase) throws IOException {
      int start = docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
      int end = docBase + parentDoc - 1; // -1 b/c parentDoc is parent doc
      return new ComplexExplanation(
          true, score(), String.format(Locale.ROOT, "Score based on child doc range from %d to %d", start, end)
      );
    }

    @Override
    public long cost() {
      return childScorer.cost();
    }
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    childQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query childRewrite = childQuery.rewrite(reader);
    Query parentRewrite = parentList.rewrite(reader);

    if (childRewrite != childQuery || parentRewrite != parentList) {
      Query rewritten = new BlockJoinParentQuery(childRewrite, parentRewrite, scoreMode);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return this;
    }
  }

  @Override
  public String toString(String field) {
    return "{!parent which='" + parentList + "'}" + childQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BlockJoinParentQuery)) return false;

    final BlockJoinParentQuery other = (BlockJoinParentQuery) o;
    return childQuery.equals(other.childQuery) &&
        parentList.equals(other.parentList) &&
        scoreMode == other.scoreMode &&
        getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    int hash = 0x4c310a59;
    hash = hash*29 + childQuery.hashCode();
    hash = hash*29 + parentList.hashCode();
    hash = hash*29 + scoreMode.hashCode();
    hash = hash*29 + Float.floatToRawIntBits(getBoost());
    return hash;
  }
}
