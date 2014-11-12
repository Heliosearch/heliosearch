/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.sindicetech.siren.search.spans;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.DocsNodesAndPositionsEnum;
import com.sindicetech.siren.index.PositionsIterator;

import java.io.IOException;

/**
 * An enumeration of spans based on term positions within a node.
 * <p>
 * Code taken from {@link org.apache.lucene.search.spans.TermSpans} and adapted for the Siren use case.
 */
class TermSpans extends Spans {

  private final DocsNodesAndPositionsEnum postings;

  private final Term term;

  private final Similarity.SimScorer docScorer;

  public TermSpans(final DocsNodesAndPositionsEnum postings, final Term term, final Similarity.SimScorer docScorer) {
    this.postings = postings;
    this.term = term;
    this.docScorer = docScorer;
  }

  // only for EmptyTermSpans (below)
  TermSpans() {
    term = null;
    postings = null;
    this.docScorer = null;
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    if (postings == null) {
      return false;
    }
    if (!postings.nextDocument()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean nextNode() throws IOException {
    if (postings == null) {
      return false;
    }
    if (!postings.nextNode()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean nextPosition() throws IOException {
    if (postings == null) {
      return false;
    }
    if (!postings.nextPosition()) {
      return false;
    }
    return true;
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    if (postings == null) {
      return false;
    }
    if (!postings.skipTo(target)) {
      return false;
    }
    return true;
  }

  @Override
  public int doc() {
    return postings.doc();
  }

  @Override
  public IntsRef node() {
    return postings.node();
  }

  @Override
  public int start() {
    return postings.pos();
  }

  @Override
  public int end() {
    return postings.pos() + 1;
  }

  @Override
  public float scoreInNode() throws IOException {
    return docScorer.score(postings.doc(), postings.termFreqInNode());
  }

  @Override
  public int getSlop() {
    return 0;
  }

  @Override
  public String toString() {
    int doc = this.doc();
    IntsRef node = this.node();
    int position = this.start();

    return "spans(" + term.toString() + ")@" +
            (doc == -1 ? "START" : (doc == DocsAndNodesIterator.NO_MORE_DOC) ? "END" : doc + "-" + node + "-" + position);
  }

  public DocsNodesAndPositionsEnum getPostings() {
    return postings;
  }

  private static final class EmptyTermSpans extends TermSpans {

    @Override
    public boolean nextCandidateDocument() throws IOException {
      return false;
    }

    @Override
    public boolean nextNode() throws IOException {
      return false;
    }

    @Override
    public boolean nextPosition() throws IOException {
      return false;
    }

    @Override
    public boolean skipToCandidate(final int target) throws IOException {
      return false;
    }

    @Override
    public int doc() {
      return DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public IntsRef node() {
      return DocsAndNodesIterator.NO_MORE_NOD;
    }

    @Override
    public int start() {
      return PositionsIterator.NO_MORE_POS;
    }

    @Override
    public int end() {
      return PositionsIterator.NO_MORE_POS;
    }

    @Override
    public float scoreInNode() throws IOException {
      return 0;
    }

  }

  public static final TermSpans EMPTY_TERM_SPANS = new EmptyTermSpans();

}
