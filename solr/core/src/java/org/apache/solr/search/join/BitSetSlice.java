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

import org.apache.solr.search.BitDocSetNative;

class BitSetSlice {
  private final BitDocSetNative bits;
  private final int off;
  private final int len;

  BitSetSlice(BitDocSetNative bits, int off, int len) {
    assert off+len <= bits.capacity();
    this.bits = bits;
    this.off = off;
    this.len = len;
  }

  public boolean get(int pos) {
    return bits.fastGet(pos + off);
  }

  public int prevSetBit(int pos) {
    int result = bits.prevSetBit(pos + off) - off;
    return (result < 0) ? -1 : result;
  }

  public int nextSetBit(int pos) {
    int result = bits.nextSetBit(pos + off) - off;
    return (result < 0 || result >= len) ? -1 : result;
  }
}

