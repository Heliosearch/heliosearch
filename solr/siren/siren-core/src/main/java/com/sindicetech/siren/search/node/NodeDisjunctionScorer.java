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
import java.util.Collection;
import java.util.List;

import org.apache.lucene.search.Weight;
import org.apache.lucene.util.IntsRef;

/**
 * A {@link NodeScorer} for OR like queries within a node, counterpart of
 * {@link NodeConjunctionScorer}.
 *
 * <p>
 *
 * Code taken from {@link DisjunctionSumScorer} and adapted for the Siren
 * use case.
 */
class NodeDisjunctionScorer extends NodeScorer {

  /** The number of subscorers. */
  private final int                      nrScorers;

  /** The scorers. */
  protected final Collection<NodeScorer> scorers;

  /**
   * The scorerNodeQueue contains all subscorers ordered by their current
   * docID(), with the minimum at the top. <br>
   * The scorerNodeQueue is initialized the first time nextDoc() or advance() is
   * called. <br>
   * An exhausted scorer is immediately removed from the scorerDocQueue. <br>
   * If less than the minimumNrMatchers scorers remain in the scorerDocQueue
   * nextDoc() and advance() return false.
   * <p>
   * After each to call to nextDoc() or advance() <code>currentScore</code> is
   * the total score of the current matching doc, <code>nrMatchers</code> is the
   * number of matching scorers, and all scorers are after the matching doc, or
   * are exhausted.
   */
  private NodeDisjunctionScorerQueue     nodeScorerQueue = null;

  /** The document number of the current match. */
  private int                            currentDoc      = -1;

  private IntsRef                        currentNode     = new IntsRef(new int[] { -1 }, 0, 1);

  /**
   * Construct a {@link NodeDisjunctionScorer}.
   *
   * @param subScorers
   *          A collection of at least two primitives scorers.
   * @throws IOException
   */
  public NodeDisjunctionScorer(final Weight weight,
                                final List<NodeScorer> scorers)
  throws IOException {
    super(weight);
    nrScorers = scorers.size();
    if (nrScorers <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }
    this.scorers = scorers;
    nodeScorerQueue  = this.initNodeScorerQueue();
  }

  /**
   * Initialize the {@link NodeDisjunctionScorerQueue}.
   */
  private NodeDisjunctionScorerQueue initNodeScorerQueue() throws IOException {
    final NodeDisjunctionScorerQueue nodeQueue = new NodeDisjunctionScorerQueue(nrScorers);
    for (final NodeScorer s : scorers) {
      nodeQueue.put(s);
    }
    return nodeQueue;
  }

  @Override
  public int freqInNode() throws IOException {
    if (currentDoc == -1) { // if nextCandidateDocument not called for the first time
      return 0;
    }
    // return the number of matchers in the node
    return this.nrMatchers();
  }

  @Override
  public float scoreInNode() throws IOException {
    if (currentDoc == -1) { // if nextCandidateDocument not called for the first time
      return 0;
    }
    nodeScorerQueue.countAndSumMatchers();
    return nodeScorerQueue.scoreInNode();
  }

  @Override
  public boolean nextCandidateDocument() throws IOException {
    boolean more = true;

    // The first time nextCandidateDocument is called, we must not advance the
    // underlying scorers as they have been already advanced during the queue init
    if (currentDoc != -1) { // if not called for the first time
      more = nodeScorerQueue.nextCandidateDocumentAndAdjustElsePop();
    }

    currentDoc = nodeScorerQueue.doc();
    currentNode = nodeScorerQueue.node();
    return more;
  }

  @Override
  public boolean nextNode() throws IOException {
    final boolean more = nodeScorerQueue.nextNodeAndAdjust();
    currentNode = nodeScorerQueue.node();
    return more;
  }

  /**
   * Returns the number of subscorers matching the current node. Initially
   * invalid, until {@link #nextCandidateDocument()} is called the first time.
   * @throws IOException
   */
  public int nrMatchers() throws IOException {
    // update the number of matched scorers
    nodeScorerQueue.countAndSumMatchers();
    return nodeScorerQueue.nrMatchersInNode();
  }

  @Override
  public boolean skipToCandidate(final int target) throws IOException {
    final boolean more = nodeScorerQueue.skipToCandidateAndAdjustElsePop(target);
    currentDoc = nodeScorerQueue.doc();
    currentNode = nodeScorerQueue.node();
    return more;
  }

  @Override
  public int doc() {
    return currentDoc;
  }

  @Override
  public IntsRef node() {
    return currentNode;
  }

  @Override
  public String toString() {
    return "NodeDisjunctionScorer(" + weight + "," + this.doc() + "," +
      this.node() + ")";
  }

}
