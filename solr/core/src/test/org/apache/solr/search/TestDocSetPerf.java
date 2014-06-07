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

package org.apache.solr.search;

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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetIterator;
import org.apache.solr.core.HS;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class TestDocSetPerf extends LuceneTestCase  {
  static Random rand = new Random(1);
  static float loadfactor;
  static SetType stype = SetType.ANY;

  enum SetType {
    ANY,
    BIT,
    INT,
    HASH,
    NINT
  }



  public FixedBitSet getRandomSet(int sz, int bitsToSet) {
    FixedBitSet bs = new FixedBitSet(sz);
    if (sz==0) return bs;
    for (int i=0; i<bitsToSet; i++) {
      bs.set(rand.nextInt(sz));
    }
    return bs;
  }

  public DocSet getHashDocSet(FixedBitSet bs) {
    int[] docs = new int[(int)bs.cardinality()];
    FixedBitSet.FixedBitSetIterator iter = new FixedBitSet.FixedBitSetIterator(bs);
    for (int i=0; i<docs.length; i++) {
      docs[i] = iter.nextDoc();
    }
    return new HashDocSet(docs,0,docs.length);
  }

  public DocSet getIntDocSet(FixedBitSet bs) {
    int[] docs = new int[(int)bs.cardinality()];
    FixedBitSet.FixedBitSetIterator iter = new FixedBitSet.FixedBitSetIterator(bs);
    for (int i=0; i<docs.length; i++) {
      docs[i] = iter.nextDoc();
    }
    return new SortedIntDocSet(docs);
  }


  public DocSet getIntDocSetNative(FixedBitSet bs) {
    int[] docs = new int[(int)bs.cardinality()];
    FixedBitSet.FixedBitSetIterator iter = new FixedBitSet.FixedBitSetIterator(bs);
    for (int i=0; i<docs.length; i++) {
      docs[i] = iter.nextDoc();
    }
    return new SortedIntDocSetNative(docs);
  }

  public DocSet getBitDocSetNative(FixedBitSet bs) {
    BitDocSetNative set = new BitDocSetNative((int)bs.length());
    FixedBitSet.FixedBitSetIterator iter = new FixedBitSet.FixedBitSetIterator(bs);
    for (;;) {
      int doc = iter.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) break;
      set.fastSet(doc);
    }
    return set;
  }

  public DocSet getSmallSet(FixedBitSet obs) {
    return getIntDocSet(obs);
  }

  public DocSet getBigSet(FixedBitSet obs) {
    return getBitDocSet(obs);
  }

  public DocSet getBitDocSet(FixedBitSet bs) {
    return new BitDocSet(bs);
  }


  public DocSet getDocSlice(FixedBitSet bs) {
    int len = (int)bs.cardinality();
    int[] arr = new int[len+5];
    arr[0]=10; arr[1]=20; arr[2]=30; arr[arr.length-1]=1; arr[arr.length-2]=2;
    int offset = 3;
    int end = offset + len;

    FixedBitSet.FixedBitSetIterator iter = new FixedBitSet.FixedBitSetIterator(bs);
    // put in opposite order... DocLists are not ordered.
    for (int i=end-1; i>=offset; i--) {
      arr[i] = iter.nextDoc();
    }

    return new DocSlice(offset, len, arr, null, len*2, 100.0f);
  }


  public DocSet getDocSet(FixedBitSet bs) {
    switch(rand.nextInt(10)) {
      case 0: return getHashDocSet(bs);

      case 1: return getBitDocSet(bs);
      case 2: return getBitDocSet(bs);
      case 3: return getBitDocSet(bs);

      case 4: return getIntDocSet(bs);
      case 5: return getIntDocSet(bs);
      case 6: return getIntDocSet(bs);
      case 7: return getIntDocSet(bs);
      case 8: return getIntDocSet(bs);

      case 9: return getDocSlice(bs);
    }
    return null;
  }




  public DocSet getRandomDocSet(int n, int maxDoc) {
    FixedBitSet obs = new FixedBitSet(maxDoc);
    int[] a = new int[n];
    for (int i=0; i<n; i++) {
      for(;;) {
        int idx = rand.nextInt(maxDoc);
        if (obs.getAndSet(idx)) continue;
        a[i]=idx;
        break;
      }
    }


    SetType setType = stype;
    if (setType == SetType.ANY) {
      if (n <= smallSetCuttoff) {
        setType = smallSetType;
      } else {
        setType = SetType.BIT;
      }
    }

    switch (setType) {
      case BIT: return new BitDocSet(obs, n);
      case INT: return getIntDocSet(obs);
      case HASH: return getHashDocSet(obs);
      case NINT: return getIntDocSetNative(obs);
      case ANY:
      default: return new BitDocSet(obs, n);
    }
  }

  public DocSet[] getRandomSets(int nSets, int minSetSize, int maxSetSize, int maxDoc) {
    DocSet[] sets = new DocSet[nSets];

    for (int i=0; i<nSets; i++) {
      int sz;
      sz = rand.nextInt(maxSetSize-minSetSize+1)+minSetSize;
      // different distribution
      // sz = (maxSetSize+1)/(rand.nextInt(maxSetSize)+1) + minSetSize;
      sets[i] = getRandomDocSet(sz,maxDoc);
    }

    return sets;
  }


  public static SetType smallSetType = SetType.INT;
  public static int smallSetCuttoff=3000;



  /****
  public void testExistsPerformance() {
    loadfactor=.75f;
    rand=new Random(12345);  // make deterministic
    int maxSetsize=4000;
    int nSets=512;
    int iter=1;
    int maxDoc=1000000;
    DocSet[] sets = getRandomHashSets(nSets,maxSetsize, maxDoc);
    int ret=0;
    long start=System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      for (DocSet s1 : sets) {
        for (int j=0; j<maxDoc; j++) {
          ret += s1.exists(j) ? 1 :0;
        }
      }
    }
    long end=System.currentTimeMillis();
    System.out.println("testExistsSizePerformance="+(end-start)+" ms");
    if (ret==-1)System.out.println("wow!");
  }
   ***/

   /**** needs code insertion into HashDocSet
   public void testExistsCollisions() {
    loadfactor=.75f;
    rand=new Random(12345);  // make deterministic
    int maxSetsize=4000;
    int nSets=512;
    int[] maxDocs=new int[] {100000,500000,1000000,5000000,10000000};
    int ret=0;

    for (int maxDoc : maxDocs) {
      int mask = (BitUtil.nextHighestPowerOfTwo(maxDoc)>>1)-1;
      DocSet[] sets = getRandomHashSets(nSets,maxSetsize, maxDoc);
      int cstart = HashDocSet.collisions;      
      for (DocSet s1 : sets) {
        for (int j=0; j<maxDocs[0]; j++) {
          int idx = rand.nextInt()&mask;
          ret += s1.exists(idx) ? 1 :0;
        }
      }
      int cend = HashDocSet.collisions;
      System.out.println("maxDoc="+maxDoc+"\tcollisions="+(cend-cstart));
    }
    if (ret==-1)System.out.println("wow!");
    System.out.println("collisions="+HashDocSet.collisions);
  }
  ***/

  public AtomicReader dummyIndexReader(final int maxDoc) {
    return new AtomicReader() {
      @Override
      public int maxDoc() {
        return maxDoc;
      }

      @Override
      public int numDocs() {
        return maxDoc;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return new FieldInfos(new FieldInfo[0]);
      }

      @Override
      public Bits getLiveDocs() {
        return null;
      }

      @Override
      public void checkIntegrity() throws IOException {

      }

      @Override
      public void addCoreClosedListener(CoreClosedListener listener) {
      }

      @Override
      public void removeCoreClosedListener(CoreClosedListener listener) {
      }

      @Override
      public Fields fields() {
        return null;
      }

      @Override
      public Fields getTermVectors(int doc) {
        return null;
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) {
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) {
        return null;
      }
      
      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) {
        return null;
      }

      @Override
      public Bits getDocsWithField(String field) throws IOException {
        return null;
      }

      @Override
      public NumericDocValues getNormValues(String field) {
        return null;
      }

      @Override
      protected void doClose() {
      }

      @Override
      public void document(int doc, StoredFieldVisitor visitor) {
      }
    };
  }

  public IndexReader dummyMultiReader(int nSeg, int maxDoc) throws IOException {
    if (nSeg==1 && rand.nextBoolean()) return dummyIndexReader(rand.nextInt(maxDoc));

    IndexReader[] subs = new IndexReader[rand.nextInt(nSeg)+1];
    for (int i=0; i<subs.length; i++) {
      subs[i] = dummyIndexReader(rand.nextInt(maxDoc));
    }

    MultiReader mr = new MultiReader(subs);
    return mr;
  }

  public static void main(String[] args) {
    new TestDocSetPerf().go(args);
  }

  public void go(String[] args) {
    rand = new Random(0);
    int a=0;
    int e = args.length;
    int iter = a==e ? 100 : Integer.parseInt(args[a++]);
    stype = a==e ? SetType.NINT : SetType.valueOf(args[a++]);
    int nSmall = a==e ? 50 : Integer.parseInt(args[a++]);
    int nLarge = a==e ? 5 : Integer.parseInt(args[a++]);
    int smallSz = a==e ? 10000 : Integer.parseInt(args[a++]);
    int largeSz = a==e ? 100000 : Integer.parseInt(args[a++]);


    int maxDoc = Math.max((int)(largeSz*1.2),1000000);
    int minBigSetSize=largeSz,maxBigSetSize=largeSz;
    int minSmallSetSize=smallSz,maxSmallSetSize=smallSz;


    // smallSetCuttoff = maxDoc>>6; // break even for SortedIntSet is /32... but /64 is better for performance
    smallSetCuttoff = maxDoc;  // make sure we always use small sets

    DocSet[] bigsets = getRandomSets(nLarge, minBigSetSize, maxBigSetSize, maxDoc);
    DocSet[] smallsets = getRandomSets(nSmall, minSmallSetSize/2, maxSmallSetSize, maxDoc);
    int ret=0;
    long start=System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      for (DocSet s1 : bigsets) {
        for (DocSet s2 : smallsets) {
          ret += s1.intersectionSize(s2);
        }
      }
    }
    long end=System.currentTimeMillis();
    System.out.println("intersectionSizePerformance="+(end-start)+" ms");
    System.out.println("ret="+ret+ " native_loaded="+ HS.loaded + " count_isizeNative=" + SortedIntDocSetNative.count_isizeNative);

    // free resources
    for (DocSet s : bigsets) s.decref();
    for (DocSet s : smallsets) s.decref();
  }



}
