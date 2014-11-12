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

package com.sindicetech.siren.index.codecs.siren10;

import java.io.IOException;

import org.apache.lucene.codecs.MultiLevelSkipListReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.IndexInput;

import com.sindicetech.siren.index.codecs.block.BlockIndexInput;

/**
 * Implementation of the {@link MultiLevelSkipListReader} for the default
 * block-based posting list format of SIREn 1.0.
 *
 * <p>
 *
 * The {@link MultiLevelSkipListReader} implementation is based on document
 * count, but it is used here with block count instead of document count.
 * In order to make it compatible with block, this class is converting
 * document count into block count and vice versa.
 *
 * @see Siren10SkipListWriter
 */
public class Siren10SkipListReader extends MultiLevelSkipListReader {

  private final BlockIndexInput.Index docIndex[];

  private final BlockIndexInput.Index lastDocIndex;

  private final int blockSize;

  Siren10SkipListReader(final IndexInput skipStream,
                        final BlockIndexInput docIn,
                        final int maxSkipLevels,
                        final int blockSkipInterval,
                        final int blockSize)
  throws IOException {
    super(skipStream, maxSkipLevels, blockSkipInterval);
    this.blockSize = blockSize;
    docIndex = new BlockIndexInput.Index[maxSkipLevels];
    for (int i = 0; i < maxSkipLevels; i++) {
      docIndex[i] = docIn.index();
    }
    lastDocIndex = docIn.index();
  }

  IndexOptions indexOptions;

  void setIndexOptions(final IndexOptions v) {
    indexOptions = v;
  }

  void init(final long skipPointer,
            final BlockIndexInput.Index docBaseIndex,
            final int blockCount) {
    super.init(skipPointer, blockCount);

    for (int i = 0; i < maxNumberOfSkipLevels; i++) {
      docIndex[i].set(docBaseIndex);
    }
  }

  @Override
  protected void seekChild(final int level) throws IOException {
    super.seekChild(level);
  }

  @Override
  protected void setLastSkipData(final int level) {
    super.setLastSkipData(level);

    lastDocIndex.set(docIndex[level]);

    if (level > 0) {
      docIndex[level-1].set(docIndex[level]);
    }
  }

  BlockIndexInput.Index getDocIndex() {
    return lastDocIndex;
  }

  @Override
  protected int readSkipData(final int level, final IndexInput skipStream) throws IOException {
    final int delta = skipStream.readVInt();
    docIndex[level].read(skipStream, false);
    return delta;
  }

  @Override
  public int skipTo(final int target) throws IOException {
    // multiply by blockSize to get the doc counts.
    return super.skipTo(target) * blockSize;
  }

}

