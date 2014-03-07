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


import org.apache.solr.core.HS;

import java.io.Closeable;
import java.io.IOException;

// An array of longs of max size 2**31 (2B long values)
public abstract class LongArray implements Closeable {
  public abstract long getSize();  // maximum index
  public abstract long getLong(int idx);
  public abstract int getInt(int idx);
  public abstract void setLong(int idx, long value);
  public abstract long memSize();


  public static LongArray create(long size, int bitsNeeded) {
    if (bitsNeeded <= 8) {
      return new LongArray8( HS.allocArray(size, 1, true) );
    } else if (bitsNeeded <= 16) {
      return new LongArray16( HS.allocArray(size, 2, true) );
    } else if (bitsNeeded <= 32) {
      return new LongArray32( HS.allocArray(size, 4, true) );
    } else {
      return new LongArray64( HS.allocArray(size, 8, true) );
    }
  }

}


// A long array backed by a native array of longs (64 bits per value)
class LongArray64 extends LongArray {
  final long arr;

  public LongArray64(long ptr) {
    arr = ptr;
  }

  @Override
  public long getSize() {
    return HS.arraySizeBytes(arr)>>3;
  }

  @Override
  public long getLong(int idx) {
    return HS.getLong(arr, idx);
  }

  @Override
  public int getInt(int idx) {
    return (int) HS.getLong(arr, idx);
  }

  @Override
  public void setLong(int idx, long value) {
    HS.setLong(arr, idx,value);
  }

  @Override
  public long memSize() {
    return HS.arraySizeBytes(arr);
  }

  @Override
  public void close() throws IOException {
    HS.freeArray(arr);
  }


}

// A long array backed by a native array of ints (32 bits per value)
class LongArray32 extends LongArray {
  final long arr;

  public LongArray32(long ptr) {
    arr = ptr;
  }

  @Override
  public long getSize() {
    return HS.arraySizeBytes(arr)>>2;
  }

  @Override
  public long getLong(int idx) {
    return HS.getInt(arr, idx);
  }

  @Override
  public int getInt(int idx) {
    return HS.getInt(arr, idx);
  }

  @Override
  public void setLong(int idx, long value) {
    HS.setInt(arr, idx, (int) value);
  }

  @Override
  public long memSize() {
    return HS.arraySizeBytes(arr);
  }

  @Override
  public void close() throws IOException {
    HS.freeArray(arr);
  }
}

// A long array backed by a native array of shorts (16 bits per value)
class LongArray16 extends LongArray {
  final long arr;

  public LongArray16(long ptr) {
    arr = ptr;
  }

  @Override
  public long getSize() {
    return HS.arraySizeBytes(arr)>>1;
  }

  @Override
  public long getLong(int idx) {
    return HS.getShort(arr, idx);
  }

  @Override
  public int getInt(int idx) {
    return HS.getShort(arr, idx);
  }

  @Override
  public void setLong(int idx, long value) {
    HS.setShort(arr, idx, (short) value);
  }

  @Override
  public long memSize() {
    return HS.arraySizeBytes(arr);
  }

  @Override
  public void close() throws IOException {
    HS.freeArray(arr);
  }
}

// A long array backed by a native array of bytes (8 bits per value)
class LongArray8 extends LongArray {
  final long arr;

  public LongArray8(long ptr) {
    arr = ptr;
  }

  @Override
  public long getSize() {
    return HS.arraySizeBytes(arr);
  }

  @Override
  public long getLong(int idx) {
    return HS.getByte(arr, idx);
  }

  @Override
  public int getInt(int idx) {
    return HS.getByte(arr, idx);
  }

  @Override
  public void setLong(int idx, long value) {
    HS.setByte(arr, idx, (byte) value);
  }

  @Override
  public long memSize() {
    return HS.arraySizeBytes(arr);
  }

  @Override
  public void close() throws IOException {
    HS.freeArray(arr);
  }
}