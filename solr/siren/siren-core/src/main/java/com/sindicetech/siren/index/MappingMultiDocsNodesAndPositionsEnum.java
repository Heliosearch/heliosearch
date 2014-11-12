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

import org.apache.lucene.codecs.MappingMultiDocsAndPositionsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MergeState.DocMap;
import org.apache.lucene.index.MultiDocsAndPositionsEnum;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.index.codecs.siren10.Siren10PostingsWriter;

/**
 * Exposes SIREn API, merged from SIREn API of sub-segments, remapping docIDs
 * (this is used for segment merging).
 *
 * @see MappingMultiDocsAndPositionsEnum
 * @see Siren10PostingsWriter#merge(MergeState, org.apache.lucene.index.DocsEnum, org.apache.lucene.util.FixedBitSet)
 */
public class MappingMultiDocsNodesAndPositionsEnum
extends DocsNodesAndPositionsEnum {

  private MultiDocsAndPositionsEnum.EnumWithSlice[] subs;
  int numSubs;
  int upto;
  DocMap currentMap;
  DocsNodesAndPositionsEnum current;
  int currentBase;
  int doc = -1;
  private MergeState mergeState;

  public MappingMultiDocsNodesAndPositionsEnum reset(final MappingMultiDocsAndPositionsEnum postingsEnum)
  throws IOException {
    this.numSubs = postingsEnum.getNumSubs();
    this.subs = postingsEnum.getSubs();
    upto = -1;
    current = null;
    return this;
  }

  public void setMergeState(final MergeState mergeState) {
    this.mergeState = mergeState;
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
          final int reader = subs[upto].slice.readerIndex;
          current = ((SirenDocsEnum) subs[upto].docsAndPositionsEnum).getDocsNodesAndPositionsEnum();
          currentBase = mergeState.docBase[reader];
          currentMap = mergeState.docMaps[reader];
        }
      }

      if (current.nextDocument()) {
        int doc = current.doc();
        if (currentMap != null) {
          // compact deletions
          doc = currentMap.get(doc);
          if (doc == -1) {
            continue;
          }
        }
        this.doc = currentBase + doc;
        return true;
      }
      else {
        current = null;
      }
    }
  }

  @Override
  public boolean nextNode() throws IOException {
    return current.nextNode();
  }

  @Override
  public boolean skipTo(final int target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int doc() {
    return doc;
  }

  @Override
  public IntsRef node() {
    return current.node();
  }

  @Override
  public boolean nextPosition() throws IOException {
    return current.nextPosition();
  }

  @Override
  public int pos() {
    return current.pos();
  }

  @Override
  public int termFreqInNode() throws IOException {
    return current.termFreqInNode();
  }

  @Override
  public int nodeFreqInDoc() throws IOException {
    return current.nodeFreqInDoc();
  }

}
