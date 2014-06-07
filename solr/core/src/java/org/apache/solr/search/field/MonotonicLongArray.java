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

import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.core.HS;

import java.io.IOException;

public class MonotonicLongArray extends LongArray {
  private final LongArray adjustments;
  private final long scaled_average_length;
  private final int offset;  //offset to the adjustment

  public static long scaleLength(double averageLength) {
    long ret = (long) (averageLength * (1<<24));  // scaled by 1<<24 and then rounded down
    assert ret > averageLength;  // make sure no overflow
    return ret;
  }

  public MonotonicLongArray(LongArray adjustments, long scaled_average_length, int offset) {
    this.adjustments = adjustments;
    this.scaled_average_length = scaled_average_length;
    this.offset = offset;
  }

  @Override
  public long getSize() {
    return adjustments.getSize();
  }

  @Override
  public long getLong(int idx) {
    int adjustment = adjustments.getInt(idx) + offset;
    return ((scaled_average_length * idx) >>> 24) + adjustment;
  }

  @Override
  public int getInt(int idx) {
    return (int)getLong(idx);
  }

  @Override
  public void setLong(int idx, long value) {
    long unadjusted = (scaled_average_length * idx) >>> 24;
    int diff = (int)(value - unadjusted);
    int adjustment = diff - offset;
    adjustments.setLong(idx, adjustment);
  }

  @Override
  public long memSize() {
    return adjustments.memSize();
  }

  @Override
  public long getNativeData() {
    return 0; // need to get wrapped array...
  }

  @Override
  public int getNativeFormat() {
    return HS.FORMAT_MONOTONIC;
  }

  @Override
  public void close() throws IOException {
    adjustments.close();
  }


  public static class Tracker {
    long numValues;
    long scaled_average_length;
    int min_diff = Integer.MAX_VALUE;
    int max_diff = Integer.MIN_VALUE;

    public Tracker(long numValues, long maxValue) {
      this.numValues = numValues;
      double averageLength = ((double)maxValue) / numValues;
      scaled_average_length = (long) (averageLength * (1<<24));  // scaled by 1<<24 and then rounded down
    }

    public void add(int idx, long value) {
      long unadjusted = (scaled_average_length * idx) >>> 24;
      int diff = (int)(value - unadjusted);
      if (diff < min_diff) {
        min_diff = diff;
      }
      if (diff > max_diff) {
        max_diff = diff;
      }
    }

    public int getRequiredBits() {
      return PackedInts.bitsRequired((long)max_diff - min_diff);
    }

    public int getMinDiff() {
      return min_diff;
    }

    public int getMaxDiff() {
      return max_diff;
    }

    public LongArray createArray() {
      int bitsRequired = getRequiredBits();
      LongArray adjustments = LongArray.create(numValues, bitsRequired);

      int offset;
      if (bitsRequired <= 8) {
        offset = (int)((1<<7) + min_diff);
      } else if (bitsRequired <= 16) {
        offset = (int)((1<<15) + min_diff);
      } else {
        return adjustments;  // use the array directly for 32/64 bit
      }

      MonotonicLongArray arr = new MonotonicLongArray(adjustments, scaled_average_length, offset);
      return arr;
    }

  }

}
