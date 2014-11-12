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

package com.sindicetech.siren.index;

import java.io.IOException;

import org.apache.lucene.index.MultiDocsAndPositionsEnum.EnumWithSlice;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.util.IntsRef;

/**
 * Exposes SIREn API, merged from SIREn API of sub-segments.
 */
public class MultiDocsNodesAndPositionsEnum extends DocsNodesAndPositionsEnum {

  private final SirenEnumWithSlice[] subs;
  int numSubs;
  int upto;
  DocsNodesAndPositionsEnum current;
  int currentBase;
  int doc = -1;

  public MultiDocsNodesAndPositionsEnum(final EnumWithSlice[] subs, final int numSubs) {
    this.numSubs = numSubs;
    this.subs = new SirenEnumWithSlice[numSubs];
    for(int i = 0; i < numSubs; i++) {
      this.subs[i] = new SirenEnumWithSlice();
      this.subs[i].docsNodesAndPositionsEnum = ((SirenDocsEnum) subs[i].docsAndPositionsEnum).getDocsNodesAndPositionsEnum();
      this.subs[i].slice = subs[i].slice;
    }
    upto = -1;
    current = null;
  }

  public int getNumSubs() {
    return numSubs;
  }

  public SirenEnumWithSlice[] getSubs() {
    return subs;
  }

  @Override
  public boolean nextDocument() throws IOException {
    while(true) {
      if (current == null) {
        if (upto == numSubs-1) {
          this.doc = NO_MORE_DOC;
          return false;
        }
        else {
          upto++;
          current = subs[upto].docsNodesAndPositionsEnum;
          currentBase = subs[upto].slice.start;
        }
      }

      if (current.nextDocument()) {
        this.doc = currentBase + current.doc();
        return true;
      }
      else {
        current = null;
      }
    }
  }

  @Override
  public boolean nextNode() throws IOException {
    if (current != null) {
      return current.nextNode();
    }
    return false;
  }

  @Override
  public boolean nextPosition() throws IOException {
    if (current != null) {
      return current.nextPosition();
    }
    return false;
  }

  @Override
  public boolean skipTo(final int target) throws IOException {
    while(true) {
      if (current != null) {
        // it is possible that the target is inferior to the current base, i.e.,
        // when the target is located in a gap between the last document of the
        // previous sub-enum and the first document of the current sub-enum.
        final int baseTarget = target < currentBase ? 0 : target - currentBase;
        if (current.skipTo(baseTarget)) {
          this.doc = current.doc() + currentBase;
          return true;
        }
        else {
          current = null;
        }
      }
      else if (upto == numSubs - 1) {
        this.doc = NO_MORE_DOC;
        return false;
      }
      else {
        upto++;
        current = subs[upto].docsNodesAndPositionsEnum;
        currentBase = subs[upto].slice.start;
      }
    }
  }

  @Override
  public int doc() {
    return doc;
  }

  @Override
  public IntsRef node() {
    if (current != null) {
      return current.node();
    }
    return NO_MORE_NOD;
  }

  @Override
  public int pos() {
    if (current != null) {
      return current.pos();
    }
    return NO_MORE_POS;
  }

  @Override
  public int termFreqInNode()
  throws IOException {
    return current.termFreqInNode();
  }

  @Override
  public int nodeFreqInDoc() throws IOException {
    return current.nodeFreqInDoc();
  }

  // TODO: implement bulk read more efficiently than super
  final class SirenEnumWithSlice {
    public DocsNodesAndPositionsEnum docsNodesAndPositionsEnum;
    public ReaderSlice slice;
  }

}
