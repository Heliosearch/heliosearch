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

import java.io.IOException;

public class MonotonicLongArray extends LongArray {
  private final LongArray adjustments;
  private final long scaled_average_length;
  private final int offset;  //offset to the adjustment to encode only positive numbers

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
  public void close() throws IOException {
    adjustments.close();
  }
}
