package org.apache.solr.search;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.HSTest;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class TestDocSetNative extends TestDocSet {

  {
    // flags to help only testing certain parts to help narrow down bugs
    intersect = true;
    union = true;
    andNot = true;
    intersectSz = true;
    unionSz = true;
    andNotSz = true;
  }

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


  public void testTracking() throws Exception {
    HSTest.startTracking();
    DocSet s1 = new SortedIntDocSetNative(new int[1]);
    boolean caught = false;
    try {
      DocSetBaseNative.debug(false);  // to test display

      HSTest.endTracking();
    } catch (Throwable th) {
      caught = true;
      SolrTestCaseJ4.log.info("SUCCESSFULLY CAUGHT: " + th);
    }
    assertTrue(caught);
    s1.decref();
  }

  public void testDoubleFree() throws Exception {
    DocSet s1 = new SortedIntDocSetNative(new int[1]);
    boolean caught = false;
    try {
      s1.decref();
      s1.decref();  // if we are really unlucky, this test could fail because the memory could be used for something else between the two decrefs...
    } catch (Throwable th) {
      caught = true;
      SolrTestCaseJ4.log.info("SUCCESSFULLY CAUGHT: " + th);
    }
    assertTrue(caught);
  }

  public void testTryWith() throws Exception {
    FixedBitSet obs = getRandomSet(10,5);

    try(
        DocSet a = getBitDocSetNative(obs);
        DocSet b = getIntDocSetNative(obs);
        DocSet c = a.intersection(b);
        DocSet d = a.union(b);
        ) {

      assertEquals(c.size(), a.intersectionSize(b));
      assertEquals(d.size(), a.unionSize(b));

    }

    // ref counting code should take care of testing this...
  }

  public void testSimple() throws Exception {
    FixedBitSet obs1 = getRandomSet(10,5);
    FixedBitSet obs2 = getRandomSet(10,5);

//    DocSet s1 = super.getHashDocSet(obs1);
//    DocSet s2 = super.getIntDocSetNative(obs2);
    DocSet s1 = super.getIntDocSetNative(obs1);
    DocSet s2 = super.getHashDocSet(obs2);

    doSingle(obs1, obs2, s1, s2);

    DocSet r1 = s1.intersection(s2);
    DocSet r2 = s2.intersection(s1);
    int c1 = s1.intersectionSize(s2);
    int c2 = s2.intersectionSize(s1);

    assertEquals(c1,c2);
    assertEquals(c1, r1.size());
    assertEquals(c1, r2.size());

    r1.decref();
    r2.decref();
    s1.decref();
    s2.decref();
  }

  /**
  @Override
  public DocSet getHashDocSet(FixedBitSet obs) {
    // return super.getHashDocSet(obs);
    // return super.getIntDocSetNative(obs);
    return super.getBitDocSetNative(obs);
  }

  @Override
  public DocSet getDocSlice(FixedBitSet obs) {
//    return super.getIntDocSetNative(obs);
    return super.getBitDocSetNative(obs);
  }
   **/

  @Override
  public DocSet getSmallSet(FixedBitSet obs) {
     return super.getIntDocSetNative(obs);
//     return super.getIntDocSetNative(obs);
//    return super.getBitDocSetNative(obs);
  }


  @Override
  public DocSet getBigSet(FixedBitSet obs) {
    // return super.getIntDocSetNative(obs);
    return super.getBitDocSetNative(obs);
  }


  public void doDedupDocSetCollector(IndexReader reader) throws Exception {
    Random r = random();
    int maxDoc = reader.maxDoc();
    int smallSetSize = (maxDoc>>6) + 5;

    FixedBitSet reference = new FixedBitSet(maxDoc);
    DedupDocSetCollector ddc = new DedupDocSetCollector(smallSetSize, maxDoc);

    for (AtomicReaderContext readerContext : reader.getContext().leaves()) {
      ddc.setNextReader(readerContext);

      int max = readerContext.reader().maxDoc();
      if (max==0) continue;
      int n = random().nextInt(max);
      int base = readerContext.docBase;
      int last=-1;
      for (int i=0; i<n; i++) {
        int doc = random().nextInt(max);
        reference.set(doc + base);

        ddc.collect(doc);
        if (random().nextInt(10) == 0) {
          if (last != -1) {
            ddc.collect(last);
          }
          if (random().nextBoolean()) {
            last = doc;
          }
        }
      }
    }

    DocSet answer = ddc.getDocSet();
    ddc.close();

    checkEqual(reference, answer);
    answer.decref();
  }


  public void testDedupDocSetCollector() throws Exception {
    // keeping these numbers smaller help hit more edge cases
    int maxSeg=4;
    int maxDoc=5;    // increase if future changes add more edge cases (like probing a certain distance in the bin search)

    for (int i=0; i<500; i++) {
      if (random().nextBoolean()) {
        maxSeg = random().nextInt(10) + 2;
        maxDoc = random().nextInt(10000) + 2;  // need something big enough to go over buffer size occasionally
      } else {
        maxSeg = 4;
        maxDoc = 5;
      }
      IndexReader r = dummyMultiReader(maxSeg, maxDoc);
      for (int j=0; j<10; j++) {
        doDedupDocSetCollector(r);
      }
    }
  }

}
