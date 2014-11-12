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
package com.sindicetech.siren.search.node;

import java.io.IOException;

import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.DocsAndNodesIterator;
import com.sindicetech.siren.index.DocsNodesAndPositionsEnum;

/**
 * A {@link NodeScorer} for nodes matching a <code>Term</code>.
 */
class NodeTermScorer extends NodePositionScorer {

  private final DocsNodesAndPositionsEnum docsEnum;

  private final Similarity.SimScorer docScorer;

  /**
   * Construct a <code>NodeTermScorer</code>.
   *
   * @param weight
   *          The weight of the <code>Term</code> in the query.
   * @param docsEnum
   *          An iterator over the documents and the positions matching the
   *          <code>Term</code>.
   * @param similarity
   *          The </code>Similarity</code> implementation to be used for score
   *          computations.
   * @param norms
   *          The field norms of the document fields for the <code>Term</code>.
   * @throws IOException
   */
  protected NodeTermScorer(final Weight weight,
                           final DocsNodesAndPositionsEnum docsEnum,
                           final Similarity.SimScorer docScorer)
  throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.docsEnum = docsEnum;
  }

  @Override
  public int doc() {
    return docsEnum.doc();
  }

  @Override
  public int freqInNode()
  throws IOException {
    return docsEnum.termFreqInNode();
  }

  @Override
  public int pos() {
    return docsEnum.pos();
  }

  @Override
  public IntsRef node() {
    return docsEnum.node();
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    return docsEnum.nextDocument();
  }

  @Override
  public boolean nextNode() throws IOException {
    return docsEnum.nextNode();
  }

  @Override
  public boolean nextPosition() throws IOException {
    return docsEnum.nextPosition();
  }

  @Override
  public float scoreInNode()
  throws IOException {
    assert this.doc() != DocsAndNodesIterator.NO_MORE_DOC;
    return docScorer.score(docsEnum.doc(), docsEnum.termFreqInNode());
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    return docsEnum.skipTo(target);
  }

  @Override
  public String toString() {
    return "NodeTermScorer(" + weight + "," + this.doc() + "," + this.node() + ")";
  }

}
