package org.apache.solr.search.field;

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


import org.apache.lucene.util.BytesRef;
import org.apache.solr.core.HS;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public final class NativePagedBytes implements Closeable {
  private final List<Long> blocks = new ArrayList<Long>();
  private final List<Integer> blockEnd = new ArrayList<Integer>();
  private int upto;
  private long currentBlock;
  private long bytesUsedInPrevBlocks;

  private final int blockSize;

  /** 1&lt;&lt;blockBits must be bigger than biggest single
   *  BytesRef slice that will be pulled */
  public NativePagedBytes(int blockBits) {
    assert blockBits > 0 && blockBits <= 31 : blockBits;
    this.blockSize = 1 << blockBits;
    upto = blockSize;
    bytesUsedInPrevBlocks = -blockSize;  // balance out upto starting at blockSize
  }

  public long getUsedSize() {
    return bytesUsedInPrevBlocks + upto;
  }

  public void copyUsingLengthPrefix(BytesRef bytes) {
    if (bytes.length >= 32768) {
      throw new IllegalArgumentException("max length is 32767 (got " + bytes.length + ")");
    }

    if (upto + bytes.length + 2 > blockSize) {
      if (bytes.length + 2 > blockSize) {
        throw new IllegalArgumentException("block size " + blockSize + " is too small to store length " + bytes.length + " bytes");
      }
      if (currentBlock != 0) {
        blocks.add(currentBlock);
        blockEnd.add(upto);
      }
      bytesUsedInPrevBlocks += upto;
      currentBlock = HS.allocArray(blockSize, 1, false);
      upto = 0;
    }

    // TODO: implement in HS for better efficiency?
    if (bytes.length < 128) {
      HS.setByte(currentBlock, upto, (byte)bytes.length);
      upto++;
    } else {
      HS.setByte(currentBlock, upto, (byte) (0x80 | (bytes.length >> 8)) );
      upto++;
      HS.setByte(currentBlock, upto, (byte) bytes.length );
      upto++;
    }

    HS.copyBytes(bytes.bytes, bytes.offset, currentBlock, upto, bytes.length);
    upto += bytes.length;
  }


  public long buildSingleArray() {
    long sz = getUsedSize();
    long arr = HS.allocArray(sz, 1, false);
    long pos = 0;
    for (int i=0; i<blocks.size(); i++) {
      long block = blocks.get(i);
      long used = blockEnd.get(i);
      HS.copyBytes(block, 0, arr, pos, used);
      pos += used;
    }
    return arr;
  }

  @Override
  public void close() throws IOException {
    for (Long block : blocks) {
      HS.freeArray(block);
    }
  }

}
