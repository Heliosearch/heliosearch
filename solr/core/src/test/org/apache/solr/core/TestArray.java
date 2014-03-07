package org.apache.solr.core;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.HSTest;
import org.apache.solr.search.field.LongArray;
import org.apache.solr.search.field.MonotonicLongArray;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestArray extends LuceneTestCase {
  private static Logger log = LoggerFactory.getLogger(TestArray.class);


  @Override
  public void setUp() throws Exception {
    super.setUp();
    HSTest.startTracking();
  }

  @Override
  public void tearDown() throws Exception {
    HSTest.endTracking();
    super.tearDown();
  }


  @Test
  public void testMonotonicArray() throws Exception {
    double averageLength = 3.2;
    long scaled_average_length = MonotonicLongArray.scaleLength(averageLength);
    int offset = 1000;   // 1000 is the lowest value we are storing

    LongArray adjustments = LongArray.create(20, 8);

    LongArray arr = new MonotonicLongArray(adjustments, scaled_average_length, offset);

    int i;

    i = 0;
    arr.setLong(i++, 1000);
    arr.setLong(i++, 1004);
    arr.setLong(i++, 1005);
    arr.setLong(i++, 1006);
    arr.setLong(i++, 1009);
    arr.setLong(i++, 1012);
    arr.setLong(i++, 1018);
    arr.setLong(i++, 1025);
    arr.setLong(i++, 1030);

    i = 0;
    assertEquals(1000, arr.getInt(i++));
    assertEquals(1004, arr.getInt(i++));
    assertEquals(1005, arr.getInt(i++));
    assertEquals(1006, arr.getInt(i++));
    assertEquals(1009, arr.getInt(i++));
    assertEquals(1012, arr.getInt(i++));
    assertEquals(1018, arr.getInt(i++));
    assertEquals(1025, arr.getInt(i++));
    assertEquals(1030, arr.getInt(i++));

    arr.close();
  }


}
