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

/**
 * Created with IntelliJ IDEA.
 * User: yonik
 * Date: 11/10/13
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHS extends LuceneTestCase {
  public void testLongArray() {
    long arr = HS.allocArray(5, 8);

    assertEquals(40, HS.arraySizeBytes(arr));

    long c1 = 0x1234567887654321L;
    long c2 = 0x8765432112345678L;

    HS.setLong(arr, 0, c1);
    HS.setLong(arr, 4, c2);

    assertEquals(c1, HS.getLong(arr, 0));
    assertEquals(c2, HS.getLong(arr, 4));

    HS.freeArray(arr);
  }

  public void testIntArray() {
    long arr = HS.allocArray(5, 4);

    assertEquals(20, HS.arraySizeBytes(arr));

    int c1 = 0x12345678;
    int c2 = 0x87654321;

    HS.setInt(arr, 0, c1);
    HS.setInt(arr, 4, c2);

    assertEquals(c1, HS.getInt(arr, 0));
    assertEquals(c2, HS.getInt(arr, 4));

    HS.freeArray(arr);
  }

  public void testAsserts() {
    long arr = HS.allocArray(5, 8);

    boolean failed=false;
    try {
      HS.setLong(arr, 5, 0L);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.getLong(arr, 5);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.setInt(arr, 10, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.getInt(arr, 10);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    //
    // negative indexes
    //

    failed=false;
    try {
      HS.setLong(arr, -1, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.getLong(arr, -1);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);
    failed=false;
    try {
      HS.setInt(arr, -1, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.getInt(arr, -1);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    //
    // zero arrays
    //
    failed=false;
    try {
      HS.setLong(0, 0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.getLong(0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    failed=false;
    try {
      HS.setInt(0, 0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);
    failed=false;
    try {
      HS.getInt(0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);


    HS.freeArray(arr);
  }


}
