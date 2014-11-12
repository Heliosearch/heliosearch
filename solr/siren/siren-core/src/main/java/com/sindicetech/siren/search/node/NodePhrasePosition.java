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

import com.sindicetech.siren.index.DocsNodesAndPositionsEnum;
import com.sindicetech.siren.index.PositionsIterator;

/**
 * Maps the current position of a {@link DocsNodesAndPositionsEnum} to a
 * position relative to the offset of the term in the phrase.
 */
class NodePhrasePosition {

  /**
   * Current position
   * <p>
   * Sentinel value is equal to {@link Integer.MIN_VALUE} since a position can
   * be negative.
   */
  int pos = Integer.MIN_VALUE;

  final int offset;            // position in phrase

  private final DocsNodesAndPositionsEnum docsEnum; // stream of positions
  protected NodePhrasePosition next;                // used to make lists

  NodePhrasePosition(final DocsNodesAndPositionsEnum docsEnum, final int offset) {
    this.docsEnum = docsEnum;
    this.offset = offset;
  }

  void init() throws IOException {
    pos = Integer.MIN_VALUE;
  }

  /**
   * Go to next location of this term current document, and set
   * <code>position</code> as <code>location - offset</code>, so that a
   * matching exact phrase is easily identified when all NodePhrasePositions
   * have exactly the same <code>position</code>.
   */
  public final boolean nextPosition() throws IOException {
    if (docsEnum.nextPosition()) {          // read subsequent pos's
      pos = docsEnum.pos() - offset;
      return true;
    }
    else {
      pos = PositionsIterator.NO_MORE_POS;
      return false;
    }
  }

  @Override
  public String toString() {
    return "NodePhrasePositions(d:"+docsEnum.doc()+" n:"+docsEnum.node()+" o:"+offset+" p:"+pos+")";
  }

}
