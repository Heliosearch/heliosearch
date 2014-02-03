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
import org.apache.solr.SolrTestCaseJ4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: yonik
 * Date: 11/10/13
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHS extends LuceneTestCase {
  private static Logger log = LoggerFactory.getLogger(TestHS.class);

  public void testLongArray() {
    long arr = HS.allocArray(5, 8, true);

    // make sure array is zeroed
    assertEquals(HS.getLong(arr, 0), 0);
    assertEquals(HS.getLong(arr, 4), 0);


    assertEquals(40, HS.arraySizeBytes(arr));

    long c1 = 0x1234567887654321L;
    long c2 = 0x8765432112345678L;

    HS.setLong(arr, 0, c1);
    HS.setLong(arr, 4, c2);

    assertEquals(c1, HS.getLong(arr, 0));
    assertEquals(c2, HS.getLong(arr, 4));

    assertTrue( HS.getNumAllocations() > HS.getNumFrees() );

    long[] arrx = new long[100];
    HS.copyLongs(arr, 2, arrx, 50, 3);
    assertEquals(c2, arrx[52]);

    HS.copyLongs(arrx, 50, arr, 1, 4);
    assertEquals(c2, HS.getLong(arr, 3));

    long arr2 = HS.allocArray(200,8, true);
    HS.copyLongs(arr, 1, arr2, 100, 4);
    for (int i=0; i<4; i++) {
      assertEquals(HS.getLong(arr, i+1), HS.getLong(arr2, i+100));
    }

    HS.freeArray(arr);
    HS.freeArray(arr2);

    assertEquals( HS.getNumAllocations() , HS.getNumFrees() );
  }

  public void testIntArray() {
    long arr = HS.allocArray(5, 4, false);

    assertEquals(20, HS.arraySizeBytes(arr));

    int c1 = 0x12345678;
    int c2 = 0x87654321;

    HS.setInt(arr, 0, c1);
    HS.setInt(arr, 4, c2);

    assertEquals(c1, HS.getInt(arr, 0));
    assertEquals(c2, HS.getInt(arr, 4));

    int[] arrx = new int[100];
    HS.copyInts(arr, 2, arrx, 50, 3);
    assertEquals(c2, arrx[52]);

    HS.copyInts(arrx, 50, arr, 1, 4);
    assertEquals(c2, HS.getInt(arr, 3));

    long arr2 = HS.allocArray(200,4, true);
    HS.copyInts(arr, 1, arr2, 100, 4);
    for (int i=0; i<4; i++) {
      assertEquals(HS.getInt(arr, i+1), HS.getInt(arr2, i+100));
    }

    HS.freeArray(arr);
    HS.freeArray(arr2);
  }

  public void testShortArray() {
    long arr = HS.allocArray(5, 2, false);

    assertEquals(10, HS.arraySizeBytes(arr));

    short c1 = (short)0x1234;
    short c2 = (short)0x8765;

    HS.setShort(arr, 0, c1);
    HS.setShort(arr, 4, c2);

    assertEquals(c1, HS.getShort(arr, 0));
    assertEquals(c2, HS.getShort(arr, 4));

    HS.freeArray(arr);
  }

  public void testByteArray() {
    long arr = HS.allocArray(5, 1, false);

    assertEquals(5, HS.arraySizeBytes(arr));

    byte c1 = (byte)0x12;
    byte c2 = (byte)0x87;

    HS.setByte(arr, 0, c1);
    HS.setByte(arr, 4, c2);

    assertEquals(c1, HS.getByte(arr, 0));
    assertEquals(c2, HS.getByte(arr, 4));

    HS.freeArray(arr);
  }



  public void testAsserts() {
    log.warn("CHECKPOINT 1");
    long arr = HS.allocArray(5, 8, false);

    boolean failed=false;
    try {
      HS.setLong(arr, 5, 0L);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 2");
    failed=false;
    try {
      HS.getLong(arr, 5);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 3");
    failed=false;
    try {
      HS.setInt(arr, 10, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 4");
    failed=false;
    try {
      HS.getInt(arr, 10);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 3a");
    failed=false;
    try {
      HS.setShort(arr, 20, (short) 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 4a");
    failed=false;
    try {
      HS.getShort(arr, 20);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 3b");
    failed=false;
    try {
      HS.setByte(arr, 40, (byte) 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 4b");
    failed=false;
    try {
      HS.getByte(arr, 40);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);



    //
    // negative indexes
    //

    log.warn("CHECKPOINT 5");
    failed=false;
    try {
      HS.setLong(arr, -1, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 6");
    failed=false;
    try {
      HS.getLong(arr, -1);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 7");
    failed=false;
    try {
      HS.setInt(arr, -1, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 8");
    failed=false;
    try {
      HS.getInt(arr, -1);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 7a");
    failed=false;
    try {
      HS.setShort(arr, -1, (short) 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 8a");
    failed=false;
    try {
      HS.getShort(arr, -1);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 7b");
    failed=false;
    try {
      HS.setByte(arr, -1, (byte) 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 8b");
    failed=false;
    try {
      HS.getByte(arr, -1);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);



    //
    // zero arrays
    //
    log.warn("CHECKPOINT 9");
    failed=false;
    try {
      HS.setLong(0, 0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 10");
    failed=false;
    try {
      HS.getLong(0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 11");
    failed=false;
    try {
      HS.setInt(0, 0, 0);
    } catch (Throwable e) {
      failed = true;
    }

    log.warn("CHECKPOINT 12");
    assertTrue(failed);
    failed=false;
    try {
      HS.getInt(0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 11a");
    failed=false;
    try {
      HS.setShort(0, 0, (short) 0);
    } catch (Throwable e) {
      failed = true;
    }

    log.warn("CHECKPOINT 12a");
    assertTrue(failed);
    failed=false;
    try {
      HS.getShort(0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);

    log.warn("CHECKPOINT 11b");
    failed=false;
    try {
      HS.setByte(0, 0, (byte) 0);
    } catch (Throwable e) {
      failed = true;
    }

    log.warn("CHECKPOINT 12b");
    assertTrue(failed);
    failed=false;
    try {
      HS.getByte(0, 0);
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);




    HS.freeArray(arr);
    log.warn("CHECKPOINT 13");



    failed=false;
    try {
      HS.freeArray(arr);  // double free... this is not guaranteed to pass since memory could be reused!!!  If this fails once in a while, simply re-run.
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);
    log.warn("CHECKPOINT 24");


    failed=false;
    try {
      HS.freeArray(0);  // null pointer free
    } catch (Throwable e) {
      failed = true;
    }
    assertTrue(failed);
    log.warn("CHECKPOINT 25");



  }


}
