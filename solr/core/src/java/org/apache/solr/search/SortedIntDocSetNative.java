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
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.core.HS;
import org.apache.solr.core.RefCount;


public class SortedIntDocSetNative extends DocSetBaseNative implements RefCount {
  protected final long array;
  protected final int len;


  @Override
  protected void free() {
    HS.freeArray(array);
  }

  /**
   * @param docs  Sorted list of ids
   */
  public SortedIntDocSetNative(int[] docs) {
    this(docs, docs.length);
  }

  /**
   * @param docs Sorted list of ids
   * @param len  Number of ids in the list
   */
  public SortedIntDocSetNative(int[] docs, int len) {
    assert len>=0 && len <= docs.length;
    this.len = len;
    array = HS.allocArray(len, 4, false);
    HS.copyInts(docs, 0, array, 0, len);
  }

  public SortedIntDocSetNative(long nativeIntArray, int len) {
    assert len>=0 && len <= (HS.arraySizeBytes(nativeIntArray)>>2);
    this.array = nativeIntArray;
    this.len = len;
  }

  @Override
  public long getNativeData() {
    return array;
  }

  @Override
  public int getNativeFormat() {
    return HS.FORMAT_INT32;
  }

  @Override
  public long getNativeSize() {
    return len;
  }

  public long getIntArrayPointer() {
    return array;
  }

  @Override
  public int size()      {
    return len;
  }

  @Override
  public long memSize() {
    return HS.arraySizeBytes(array)+8;
  }


  public static native int intersectionSizeNative(long smallerSortedList, int a_size, long biggerSortedList, int b_size);
  public static long count_isizeNative; // debugging count

  public static int intersectionSize(long smallerSortedList, int a_size, long biggerSortedList, int b_size) {
    final long a = smallerSortedList;
    final long b = biggerSortedList;

    // The next doc we are looking for will be much closer to the last position we tried
    // than it will be to the midpoint between last and high... so probe ahead using
    // a function of the ratio of the sizes of the sets.
    int step = (b_size/a_size)+1;

    // Since the majority of probes should be misses, we'll already be above the last probe
    // and shouldn't need to move larger than the step size on average to step over our target (and thus lower
    // the high upper bound a lot.)... but if we don't go over our target, it's a big miss... so double it.
    step = step + step;

    // FUTURE: come up with a density such that target * density == likely position?
    // then check step on one side or the other?
    // (density could be cached in the DocSet)... length/maxDoc

    // FUTURE: try partitioning like a sort algorithm.  Pick the midpoint of the big
    // array, find where that should be in the small array, and then recurse with
    // the top and bottom half of both arrays until they are small enough to use
    // a fallback insersection method.
    // NOTE: I tried this and it worked, but it was actually slower than this current
    // highly optimized approach.

    int icount = 0;
    int low = 0;
    int max = b_size-1;

    for (int i=0; i<a_size; i++) {
      int doca = HS.getInt(a, i);

      int high = max;

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (HS.getInt(b,probe)>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (HS.getInt(b,probe)>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }

      // binary search the rest of the way
      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = HS.getInt(b,mid);

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          icount++;
          low = mid+1;  // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }


  public static boolean intersects(long smallerSortedList, int a_size, long biggerSortedList, int b_size) {
    // see intersectionSize for more in-depth comments of this algorithm

    final long a = smallerSortedList;
    final long b = biggerSortedList;

    int step = (b_size/a_size)+1;

    step = step + step;

    int low = 0;
    int max = b_size-1;

    for (int i=0; i<a_size; i++) {
      int doca = HS.getInt(a, i);
      int high = max;
      int probe = low + step;
      if (probe<high) {
        if (HS.getInt(b, probe) >= doca) {
          high=probe;
        } else {
          low=probe+1;
          probe = low + step;
          int probeVal = HS.getInt(b, probe);
          if (probeVal < high) {
            if (probeVal >= doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }

      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = HS.getInt(b, mid);

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          return true;
        }
      }
    }

    return false;
  }


  @Override
  public int intersectionSize(DocSet other) {
    if (!(other instanceof SortedIntDocSetNative)) {
      // assume other implementations are better at random access than we are,
      // true of BitDocSet and HashDocSet.
      int icount = 0;
      for (int i=0; i<len; i++) {
        if (other.exists( HS.getInt(array,i) )) icount++;
      }
      return icount;
    }

    // make "a" the smaller set.
    SortedIntDocSetNative otherNative = ((SortedIntDocSetNative)other);
    int a_size = size();
    int b_size = otherNative.size();
    long a,b;

    if (a_size <= b_size) {
      a = array;
      b = otherNative.array;
    } else {
      a = otherNative.array;
      b = array;
      a_size = b_size;
      b_size = size();
    }

    if (a_size==0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b_size>>3) >= a_size) {
      if (HS.loaded) {
        count_isizeNative++;
        return intersectionSizeNative(a,a_size,b,b_size);
      } else {
        return intersectionSize(a,a_size, b,b_size);
      }
    }

    // TODO: move this to native code as well...
    // if they are close in size, just do a linear walk of both.
    int icount=0;
    int i=0,j=0;
    int doca = HS.getInt(a, i);
    int docb = HS.getInt(b, j);
    for(;;) {
      // switch on the sign bit somehow?  Hopefully JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b_size) break;
        docb=HS.getInt(b,j);
      } else if (doca < docb) {
        if (++i >= a_size) break;
        doca=HS.getInt(a,i);
      } else {
        icount++;
        if (++i >= a_size) break;
        doca=HS.getInt(a,i);
        if (++j >= b_size) break;
        docb=HS.getInt(b,j);
      }
    }
    return icount;
  }

  @Override
  public boolean intersects(DocSet other) {
    if (!(other instanceof SortedIntDocSetNative)) {
      // assume other implementations are better at random access than we are,
      // true of BitDocSet and HashDocSet.
      for (int i=0; i<len; i++) {
        if (other.exists( HS.getInt(array,i) )) return true;
      }
      return false;
    }

    // make "a" the smaller set.
    SortedIntDocSetNative otherNative = ((SortedIntDocSetNative)other);
    int a_size = size();
    int b_size = otherNative.size();
    long a,b;

    if (a_size <= b_size) {
      a = array;
      b = otherNative.array;
    } else {
      a = otherNative.array;
      a_size = b_size;
      b = array;
      b_size = size();
    }

    if (a_size==0) return false;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((b_size>>3) >= a_size) {
      return intersects(a, a_size, b, b_size);
    }

    // if they are close in size, just do a linear walk of both.
    int i=0,j=0;
    int doca=HS.getInt(a,i), docb=HS.getInt(b,j);
    for(;;) {
      // switch on the sign bit somehow?  Hopefull JVM is smart enough to just test once.

      // Since set a is less dense then set b, doca is likely to be greater than docb so
      // check that case first.  This resulted in a 13% speedup.
      if (doca > docb) {
        if (++j >= b_size) break;
        docb=HS.getInt(b,j);
      } else if (doca < docb) {
        if (++i >= a_size) break;
        doca=HS.getInt(a,i);
      } else {
        return true;
      }
    }
    return false;
  }

  /** puts the intersection of a and b into the target array and returns the size */
  public static int intersection(long a, int lena, long b, int lenb, int[] target) {
    if (lena > lenb) {
      int ti=lena; lena=lenb; lenb=ti;
      long ta=a; a=b; b=ta;
    }

    if (lena==0) return 0;

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb>>3) >= lena) {
      return intersectionBinarySearch(a, lena, b, lenb, target);
    }

    int icount=0;
    int i=0,j=0;
    int doca=HS.getInt(a,i), docb=HS.getInt(b,j);
    for(;;) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb=HS.getInt(b,j);
      } else if (doca < docb) {
        if (++i >= lena) break;
        doca=HS.getInt(a,i);
      } else {
        target[icount++] = doca;
        if (++i >= lena) break;
        doca=HS.getInt(a,i);
        if (++j >= lenb) break;
        docb=HS.getInt(b,j);
      }
    }
    return icount;
  }

  /** Puts the intersection of a and b into the target array and returns the size.
   * lena should be smaller than lenb */
  protected static int intersectionBinarySearch(long a, int lena, long b, int lenb, int[] target) {
    int step = (lenb/lena)+1;
    step = step + step;


    int icount = 0;
    int low = 0;
    int max = lenb-1;

    for (int i=0; i<lena; i++) {
      int doca = HS.getInt(a,i);

      int high = max;

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (HS.getInt(b,probe)>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (HS.getInt(b,probe)>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }


      // binary search
      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = HS.getInt(b,mid);

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          target[icount++]=doca;
          // HS.setInt(target, icount++, doca);
          low = mid+1;  // found it, so start at next element
          break;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
    }

    return icount;
  }

  @Override
  public DocSet intersection(DocSet other) {
    if (!(other instanceof SortedIntDocSetNative)) {
      int icount = 0;
      int arr[] = new int[len];
      for (int i=0; i<len; i++) {
        int doc = HS.getInt(array, i);
        if (other.exists(doc)) arr[icount++] = doc;
      }
      return new SortedIntDocSetNative(arr,icount);
    }

    long otherDocs = ((SortedIntDocSetNative)other).array;
    int maxsz = Math.min(len, ((SortedIntDocSetNative)other).len);
    int[] arr = new int[maxsz];
    int sz = intersection(array, len, otherDocs, ((SortedIntDocSetNative)other).len, arr);
    return new SortedIntDocSetNative(arr,sz);
  }


  protected static int andNotBinarySearch(long a, int lena, long b, int lenb, int[] target) {
   int step = (lenb/lena)+1;
    step = step + step;


    int count = 0;
    int low = 0;
    int max = lenb-1;

    outer:
    for (int i=0; i<lena; i++) {
      int doca = HS.getInt(a,i);

      int high = max;

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (HS.getInt(b,probe)>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (HS.getInt(b,probe)>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }


      // binary search
      while (low <= high) {
        int mid = (low+high) >>> 1;
        int docb = HS.getInt(b,mid);

        if (docb < doca) {
          low = mid+1;
        }
        else if (docb > doca) {
          high = mid-1;
        }
        else {
          low = mid+1;  // found it, so start at next element
          continue outer;
        }
      }
      // Didn't find it... low is now positioned on the insertion point,
      // which is higher than what we were looking for, so continue using
      // the same low point.
      target[count++] = doca;
    }

    return count;
  }

    /** puts the intersection of a and not b into the target array and returns the size */
  public static int andNot(long a, int lena, long b, int lenb, int[] target) {
    if (lena==0) return 0;
    if (lenb==0) {
      HS.copyInts(a, 0, target, 0, lena);
      return lena;
    }

    // if b is 8 times bigger than a, use the modified binary search.
    if ((lenb>>3) >= lena) {
      return andNotBinarySearch(a, lena, b, lenb, target);
    }

    int count=0;
    int i=0,j=0;
    int doca=HS.getInt(a,i),docb=HS.getInt(b,j);
    for(;;) {
      if (doca > docb) {
        if (++j >= lenb) break;
        docb=HS.getInt(b,j);
      } else if (doca < docb) {
        target[count++] = doca;
        if (++i >= lena) break;
        doca=HS.getInt(a,i);
      } else {
        if (++i >= lena) break;
        doca=HS.getInt(a,i);
        if (++j >= lenb) break;
        docb=HS.getInt(b,j);
      }
    }

    int leftover=lena - i;

    if (leftover > 0) {
      HS.copyInts(a, i, target, count, leftover);
      count += leftover;
    }

    return count;
  }

  @Override
  public DocSet andNot(DocSet other) {
    if (other.size()==0) {
      this.incref();
      return this;
    }

    if (!(other instanceof SortedIntDocSetNative)) {
      int count = 0;
      int arr[] = new int[len];
      for (int i=0; i<len; i++) {
        int doc = HS.getInt(array, i);
        if (!other.exists(doc)) arr[count++] = doc;
      }
      return new SortedIntDocSetNative(arr,count);
    }

    long otherDocs = ((SortedIntDocSetNative)other).array;
    int[] arr = new int[len];
    int sz = andNot(array, len, otherDocs, ((SortedIntDocSetNative)other).len, arr);
    return new SortedIntDocSetNative(arr,sz);
  }

  @Override
  public void setBitsOn(FixedBitSet target) {
    for (int i=0; i<len; i++) {
      target.set( HS.getInt(array, i) );
    }
  }


  @Override
  public boolean exists(int doc) {
    // this could be faster by estimating where in the list the doc is likely to appear,
    // but we should get away from using exists() anyway.
    int low = 0;
    int high = len-1;
    // binary search
    while (low <= high) {
      int mid = (low+high) >>> 1;
      int docb = HS.getInt(array, mid);

      if (docb < doc) {
        low = mid+1;
      }
      else if (docb > doc) {
        high = mid-1;
      }
      else {
        return true;
      }
    }
    return false;
  }
  

  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      int pos=0;
      @Override
      public boolean hasNext() {
        return pos < len;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      /**
       * The remove  operation is not supported by this Iterator.
       */
      @Override
      public void remove() {
        throw new UnsupportedOperationException("The remove  operation is not supported by this Iterator.");
      }

      @Override
      public int nextDoc() {
        return HS.getInt(array, pos++);
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }

  @Override
  public FixedBitSet getBits() {   // TODO: change to native?
    int maxDoc = size() > 0 ? HS.getInt(array,len-1) : 0;    // WARNING!!!  can't used fixed bit sizes here!
    FixedBitSet bs = new FixedBitSet(maxDoc+1);
    setBitsOn(bs);
    return bs;
  }


  private static int findIndex(long arr, int value, int low, int high) {
    // binary search
    while (low <= high) {
      int mid = (low+high) >>> 1;
      int found = HS.getInt(arr,mid);

      if (found < value) {
        low = mid+1;
      }
      else if (found > value) {
        high = mid-1;
      }
      else {
        return mid;
      }
    }
    return low;
  }

  @Override
  public Filter getTopFilter() {
    return new Filter() {
      int lastEndIdx = 0;

      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) {
        AtomicReader reader = context.reader();
        // all Solr DocSets that are used as filters already only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.
        int sidx = Math.max(0,lastEndIdx);

        if (sidx > 0 && HS.getInt(array,sidx-1) >= base) {
          // oops, the lastEndIdx isn't correct... we must have been used
          // in a multi-threaded context, or the indexreaders are being
          // used out-of-order.  start at 0.
          sidx = 0;
        }
        if (sidx < len && HS.getInt(array,sidx) < base) {
          // if docs[sidx] is < base, we need to seek to find the real start.
          sidx = findIndex(array, base, sidx, len-1);
        }

        final int startIdx = sidx;

        // Largest possible end index is limited to the start index
        // plus the number of docs contained in the segment.  Subtract 1 since
        // the end index is inclusive.
        int eidx = Math.min(len, startIdx + maxDoc) - 1;

        // find the real end
        eidx = findIndex(array, max, startIdx, eidx) - 1;

        final int endIdx = eidx;
        lastEndIdx = endIdx;


        return BitsFilteredDocIdSet.wrap(new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int idx = startIdx;
              int adjustedDoc = -1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() {
                return adjustedDoc = (idx > endIdx) ? NO_MORE_DOCS : (HS.getInt(array,idx++) - base);
              }

              @Override
              public int advance(int target) {
                if (idx > endIdx || target==NO_MORE_DOCS) return adjustedDoc=NO_MORE_DOCS;
                target += base;

                // probe next
                int rawDoc = HS.getInt(array,idx++);
                if (rawDoc >= target) return adjustedDoc=rawDoc-base;

                int high = endIdx;

                // TODO: probe more before resorting to binary search?

                // binary search
                while (idx <= high) {
                  int mid = (idx+high) >>> 1;
                  rawDoc = HS.getInt(array,mid);

                  if (rawDoc < target) {
                    idx = mid+1;
                  }
                  else if (rawDoc > target) {
                    high = mid-1;
                  }
                  else {
                    idx=mid+1;
                    return adjustedDoc=rawDoc - base;
                  }
                }

                // low is on the insertion point...
                if (idx <= endIdx) {
                  return adjustedDoc = HS.getInt(array,idx++) - base;
                } else {
                  return adjustedDoc=NO_MORE_DOCS;
                }
              }

              @Override
              public long cost() {
                return len;
              }
            };
          }

          @Override
          public boolean isCacheable() {
            return true;
          }

          @Override
          public Bits bits() {
            // random access is expensive for this set
            return null;
          }

        }, acceptDocs2);
      }
    };
  }

  @Override
  public SortedIntDocSetNative clone() {
    long newArr = HS.allocArray(len, 4, false);
    HS.copyInts(array, 0, newArr, 0, len);
    return new SortedIntDocSetNative(newArr, len);
  }
}
