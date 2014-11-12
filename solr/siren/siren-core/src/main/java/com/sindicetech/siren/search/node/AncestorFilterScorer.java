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

import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.DocsAndNodesIterator;

import java.io.IOException;

/**
 * A {@link NodeScorer} that filters node path and output the ancestor node path.
 *
 * <p>
 *
 * The level of the ancestor must be specified. It is used to modify the
 * {@link IntsRef#length} of the node label returned by the inner scorer.
 */
class AncestorFilterScorer extends NodeScorer {

  private final NodeScorer scorer;
  private final int ancestorLevel;

  public AncestorFilterScorer(final NodeScorer scorer, final int ancestorLevel) {
    super(scorer.getWeight());
    this.scorer = scorer;
    this.ancestorLevel = ancestorLevel;
  }

  protected NodeScorer getScorer() {
    return scorer;
  }

  @Override
  public int freqInNode() throws IOException {
    return scorer.freqInNode();
  }

  @Override
  public float scoreInNode()
  throws IOException {
    return scorer.scoreInNode();
  }

  @Override
  public String toString() {
    return "AncestorFilterScorer(" + weight + "," + this.doc() + "," +
      this.node() + ")";
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    return scorer.nextCandidateDocument();
  }

  @Override
  public boolean nextNode() throws IOException {
    return scorer.nextNode();
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    return scorer.skipToCandidate(target);
  }

  @Override
  public int doc() {
    return scorer.doc();
  }

  @Override
  public IntsRef node() {
    final IntsRef node = scorer.node();
    // resize node array only if node is not a sentinel value
    if (node.length > ancestorLevel &&
        node.ints[node.offset] != -1 &&
        node != DocsAndNodesIterator.NO_MORE_NOD) {
      node.length = ancestorLevel;
    }
    return node;
  }

}
