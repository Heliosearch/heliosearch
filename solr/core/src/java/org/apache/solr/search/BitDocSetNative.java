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
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetIterator;
import org.apache.solr.core.HS;

import java.io.IOException;
import java.util.Arrays;


public class BitDocSetNative extends DocSetBaseNative implements Cloneable  {
  final long array;
  protected final int wlen; // number of words in the array
  protected int size = -1;  // number of docs in the set (cached for perf)

  @Override
  protected void free() {
    HS.freeArray(array);
  }

  public BitDocSetNative(int numBits) {
    this.wlen = OpenBitSet.bits2words(numBits);
    this.array = HS.allocArray(wlen, 8, true);
  }

  public BitDocSetNative(BitDocSetNative other) {
    this.wlen = other.wlen;
    this.array = HS.allocArray(wlen, 8, false);  // don't zero memory since we will copy over it
    HS.copyLongs(other.array, 0, array, 0, wlen);
    // Don't set size... the purpose of making a copy will be to change it.
  }

  public BitDocSetNative(OpenBitSet other) {
    this.wlen = other.getNumWords();
    this.array = HS.allocArray(wlen, 8, false);  // don't zero memory since we will copy over it
    HS.copyLongs(other.getBits(), 0, this.array, 0, wlen);
  }

  public OpenBitSet toOpenBitSet() {
    long[] longArray = new long[wlen];
    HS.copyLongs(array, 0, longArray, 0, wlen);
    return new OpenBitSet(longArray, wlen);
    // Don't set size... the purpose of making a copy will be to change it.
  }

  public int capacity() {
    return wlen<<6;
  }

  public DocIterator iterator() {
    return new DocIterator() {
      int pos=nextSetBit(0);
      public boolean hasNext() {
        return pos>=0;
      }

      public Integer next() {
        return nextDoc();
      }

      public void remove() {
        fastClear(pos);
      }

      public int nextDoc() {
        int old=pos;
        pos=nextSetBit(old + 1);
        return old;
      }

      public float score() {
        return 0.0f;
      }
    };
  }

  /***
  @Override
  public DocIterator iterator() {
    return new DocIterator() {
      private final OpenBitSetIterator iter = new OpenBitSetIterator(bits);
      private int pos = iter.nextDoc();
      @Override
      public boolean hasNext() {
        return pos != DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public Integer next() {
        return nextDoc();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
        // bits.clear(pos);
      }

      @Override
      public int nextDoc() {
        int old=pos;
        pos=iter.nextDoc();
        return old;
      }

      @Override
      public float score() {
        return 0.0f;
      }
    };
  }
  ***/

  @Override
  public OpenBitSet getBits() {
    // HS-TODO: if used in production, we should optimize
    OpenBitSet obs = new OpenBitSet(capacity());
    long[] target = obs.getBits();
    for (int i=0; i<wlen; i++) {
      target[i] = HS.getLong(array, i);
    }
    return obs;
  }

  @Override
  public void add(int doc) {
    fastSet(doc);
    size=-1;  // invalidate size
  }

  @Override
  public void addUnique(int doc) {
    fastSet(doc);
    size=-1;  // invalidate size
  }

  public int numWords() {
    return wlen;
  }

  @Override
  public int size() {
    if (size!=-1) return size;
    int words = numWords();
    int nBits = 0;
    for (int i=0; i<words; i++) {
      nBits += Long.bitCount( HS.getLong(array, i) );
    }
    size = nBits;
    return size;
  }

  /**
   * The number of set bits - size - is cached.  If the bitset is changed externally,
   * this method should be used to invalidate the previously cached size.
   */
  public void invalidateSize() {
    size=-1;
  }

  /** expert:
   * Sets the number of bits set.  This is not validated!
   */
  public void setSize(int size) {
    this.size = size;
  }


  /** Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean fastGet(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (HS.getLong(array, i) & bitmask) != 0;
  }

  /** returns 1 if the bit is set, 0 if not.
   * The index should be less than the OpenBitSet size
   */
  public int getBit(int index) {
    int i = index >> 6;                // div 64
    int bit = index & 0x3f;            // mod 64
    return ((int)(HS.getLong(array,i)>>>bit)) & 0x01;
  }

  public void fastSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    HS.setLong(array,wordNum, HS.getLong(array,wordNum) | bitmask);
  }

  public void fastClear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;

    HS.setLong(array, wordNum, HS.getLong(array, wordNum) & ~bitmask);
    // hmmm, it takes one more instruction to clear than it does to set... any
    // way to work around this?  If there were only 63 bits per word, we could
    // use a right shift of 10111111...111 in binary to position the 0 in the
    // correct place (using sign extension).
    // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
    // by the JVM into a native instruction.
    // bits[word] &= Long.rotateLeft(0xfffffffe,bit);
  }

  public boolean getAndSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (HS.getLong(array,wordNum) & bitmask) != 0;
    HS.setLong(array,wordNum, HS.getLong(array,wordNum) | bitmask);
    return val;
  }

  public void fastFlip(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    HS.setLong(array,wordNum, HS.getLong(array,wordNum) ^ bitmask);
  }

  public boolean flipAndGet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    long word = HS.getLong(array,wordNum) ^ bitmask;
    HS.setLong(array, wordNum, word);
    return (word & bitmask) != 0;
  }

  public void flip(int startIndex, int endIndex) {
    if (endIndex <= startIndex) return;
    int startWord = (startIndex>>6);

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord = (endIndex-1) >> 6;

    /*** Grrr, java shifting wraps around so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
     long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
     long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
     ***/

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    if (startWord == endWord) {
      HS.setLong(array, startWord, HS.getLong(array, startWord) ^ (startmask & endmask) );
      return;
    }

    HS.setLong(array, startWord, HS.getLong(array, startWord) ^ startmask);

    for (int i=startWord+1; i<endWord; i++) {
      HS.setLong(array, i, ~HS.getLong(array, i));
    }

    HS.setLong(array, endWord, HS.getLong(array, endWord) ^ endmask);
  }

  /** @return the number of set bits */
  public long cardinality() {
    return size();
  }

  public static boolean intersects(BitDocSetNative a, BitDocSetNative b) {
    assert(a.wlen == b.wlen);
    int nWords = a.wlen;
    for (int i=0; i<nWords; i++) {
      if ( (HS.getLong(a.array, i) & HS.getLong(b.array, i)) != 0 ) {
        return true;
      }
    }
    return false;
  }

  /** Returns the popcount or cardinality of the intersection of the two sets.
   * Neither set is modified.
   */
  public static int intersectionCount(BitDocSetNative a, BitDocSetNative b) {
    assert(a.wlen == b.wlen);
    int nWords = a.numWords();
    int result = 0;
    for (int i=0; i<nWords; i++) {
      long w1 = HS.getLong(a.array, i);
      long w2 = HS.getLong(b.array, i);
      result += Long.bitCount(w1 & w2);
    }
    return result;
  }

  public static int intersectionCount(BitDocSetNative a, OpenBitSet b) {
    assert(a.wlen == b.getNumWords());
    int nWords = a.numWords();
    long[] bArray = b.getBits();
    int result = 0;
    for (int i=0; i<nWords; i++) {
      long w1 = HS.getLong(a.array, i);
      long w2 = bArray[i];
      result += Long.bitCount(w1 & w2);
    }
    return result;
  }

  public static int unionCount(BitDocSetNative a, BitDocSetNative b) {
    assert(a.wlen == b.wlen);
    int nWords = a.numWords();
    int result = 0;
    for (int i=0; i<nWords; i++) {
      long w1 = HS.getLong(a.array, i);
      long w2 = HS.getLong(b.array, i);
      result += Long.bitCount(w1 | w2);
    }
    return result;
  }

  public static int andNotCount(BitDocSetNative a, BitDocSetNative b) {
    assert(a.wlen == b.wlen);
    int nWords = a.numWords();
    int result = 0;
    for (int i=0; i<nWords; i++) {
      long w1 = HS.getLong(a.array, i);
      long w2 = HS.getLong(b.array, i);
      result += Long.bitCount(w1 & ~w2);
    }
    return result;
  }

  public static int xorCount(BitDocSetNative a, BitDocSetNative b) {
    assert(a.wlen == b.wlen);
    int nWords = a.numWords();
    int result = 0;
    for (int i=0; i<nWords; i++) {
      long w1 = HS.getLong(a.array, i);
      long w2 = HS.getLong(b.array, i);
      result += Long.bitCount(w1 ^ w2);
    }
    return result;
  }



  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    int i = index>>6;
    if (i>=wlen) return -1;
    int subIndex = index & 0x3f;      // index within the word
    long word = HS.getLong(array, i) >> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (i<<6) + subIndex + Long.numberOfTrailingZeros(word);
    }

    while(++i < wlen) {
      word = HS.getLong(array, i);
      if (word!=0) return (i<<6) + Long.numberOfTrailingZeros(word);
    }

    return -1;
  }


  /** Returns the index of the first set bit starting downwards at
   *  the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int prevSetBit(int index) {
    int i = index >> 6;
    final int subIndex;
    long word;
    if (i >= wlen) {
      i = wlen - 1;
      if (i < 0) return -1;
      subIndex = 63;  // last possible bit
      word = HS.getLong(array,i);
    } else {
      if (i < 0) return -1;
      subIndex = index & 0x3f;  // index within the word
      word = (HS.getLong(array,i) << (63-subIndex));  // skip all the bits to the left of index
    }

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = HS.getLong(array, i);
      if (word !=0 ) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }



  /** this = this AND other */
  public void intersectMe(BitDocSetNative other) {
    assert this.wlen == other.wlen;
    long thisArr = this.array;
    long otherArr = other.array;
    // testing against zero can be more efficient
    int pos=wlen;
    while(--pos>=0) {
      HS.setLong(thisArr, pos, HS.getLong(thisArr,pos) & HS.getLong(otherArr, pos));
    }
  }

  /** this = this OR other */
  public void unionMe(BitDocSetNative other) {
    assert this.wlen == other.wlen;
    long thisArr = this.array;
    long otherArr = other.array;
    // testing against zero can be more efficient
    int pos=wlen;
    while(--pos>=0) {
      HS.setLong(thisArr, pos, HS.getLong(thisArr,pos) | HS.getLong(otherArr, pos));
    }
  }

  /** Remove all elements set in other. this = this AND_NOT other */
  public void remove(BitDocSetNative other) {
    assert this.wlen == other.wlen;
    long thisArr = this.array;
    long otherArr = other.array;
    // testing against zero can be more efficient
    int pos=wlen;
    while(--pos>=0) {
      HS.setLong(thisArr, pos, HS.getLong(thisArr,pos) & ~HS.getLong(otherArr, pos));
    }
  }

  /** Remove all elements set in other. this = this AND_NOT other */
  public void xorMe(BitDocSetNative other) {
    assert this.wlen == other.wlen;
    long thisArr = this.array;
    long otherArr = other.array;
    // testing against zero can be more efficient
    int pos=wlen;
    while(--pos>=0) {
      HS.setLong(thisArr, pos, HS.getLong(thisArr,pos) ^ HS.getLong(otherArr, pos));
    }
  }


  ///////////////////////////////////////////////////////////////////////
  ////////////////////////////// DocSet methods /////////////////////////
  ///////////////////////////////////////////////////////////////////////

  /** Returns true of the doc exists in the set.
   *  Should only be called when doc < OpenBitSet.size()
   */
  @Override
  public boolean exists(int doc) {
    return fastGet(doc);
  }

  @Override
  public int intersectionSize(DocSet other) {
    if (other instanceof BitDocSetNative) {
      return intersectionCount(this, ((BitDocSetNative)other));
    } else {
      // they had better not call us back!
      return other.intersectionSize(this);
    }
  }

  @Override
  public boolean intersects(DocSet other) {
    if (other instanceof BitDocSetNative) {
      return intersects(this, ((BitDocSetNative) other));
    } else {
      // they had better not call us back!
      return other.intersects(this);
    }
  }

  @Override
  public int unionSize(DocSet other) {
    if (other instanceof BitDocSetNative) {
      // if we don't know our current size, this is faster than
      // size + other.size - intersection_size
      return unionCount(this, ((BitDocSetNative)other));
    } else {
      // they had better not call us back!
      return other.unionSize(this);
    }
  }

  @Override
  public int andNotSize(DocSet other) {
    if (other instanceof BitDocSetNative) {
      // if we don't know our current size, this is faster than
      // size - intersection_size
      return andNotCount(this, ((BitDocSetNative)other));
    } else {
      return super.andNotSize(other);
    }
  }

  @Override
  public void setBitsOn(OpenBitSet target) {
    assert this.wlen == target.getNumWords();
    long thisArr = this.array;
    long[] otherArr = target.getBits();
    // testing against zero can be more efficient
    int pos=wlen;
    while(--pos>=0) {
      otherArr[pos] |= HS.getLong(thisArr, pos);
    }
  }

  @Override
  public DocSet andNot(DocSet other) {
    BitDocSetNative newbits = clone();
     if (other instanceof BitDocSetNative) {
       newbits.remove(((BitDocSetNative) other));
     } else {
       DocIterator iter = other.iterator();
       while (iter.hasNext()) newbits.fastClear(iter.nextDoc());
     }
     return newbits;
  }

  @Override
   public DocSet union(DocSet other) {
    BitDocSetNative newbits = clone();
     if (other instanceof BitDocSetNative) {
       newbits.unionMe(((BitDocSetNative) other));
     } else {
       DocIterator iter = other.iterator();
       while (iter.hasNext()) newbits.fastSet(iter.nextDoc());
     }
     return newbits;
  }

  @Override
  public long memSize() {
    return (((long)wlen) << 3) + 16;
  }

  @Override
  public BitDocSetNative clone() {
    return new BitDocSetNative(this);
  }

  /***  HS-TODO.. change DocSet to extend / encompass DocIdSet?
  // hopefully temporary
  public DocIdSet getDocIdSet() {
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return getDocIdSetIterator();
      }
    }
  }

  // hopefully temporary
  public DocIdSetIterator getDocIdSetIterator() {

  }
   ***/

  @Override
  public Filter getTopFilter() {
    final BitDocSetNative bs = this;
    // TODO: if cardinality isn't cached, do a quick measure of sparseness
    // and return null from bits() if too sparse.

    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) {
        AtomicReader reader = context.reader();
        // all Solr DocSets that are used as filters only include live docs
        final Bits acceptDocs2 = acceptDocs == null ? null : (reader.getLiveDocs() == acceptDocs ? null : acceptDocs);

        /*** HS-TODO
        if (context.isTopLevel) {
          return BitsFilteredDocIdSet.wrap(bs, acceptDocs);
        }
        ***/

        final int base = context.docBase;
        final int maxDoc = reader.maxDoc();
        final int max = base + maxDoc;   // one past the max doc in this segment.

        return BitsFilteredDocIdSet.wrap(new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int pos=base-1;
              int adjustedDoc=-1;

              @Override
              public int docID() {
                return adjustedDoc;
              }

              @Override
              public int nextDoc() {
                pos = bs.nextSetBit(pos+1);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public int advance(int target) {
                if (target==NO_MORE_DOCS) return adjustedDoc=NO_MORE_DOCS;
                pos = bs.nextSetBit(target+base);
                return adjustedDoc = (pos>=0 && pos<max) ? pos-base : NO_MORE_DOCS;
              }

              @Override
              public long cost() {
                // we don't want to actually compute cardinality, but
                // if its already been computed, we use it (pro-rated for the segment)
                if (size != -1) {
                  return (long)(size * ((OpenBitSet.bits2words(maxDoc)<<6) / (float)bs.capacity()));
                } else {
                  return maxDoc;
                }
              }
            };
          }

          @Override
          public boolean isCacheable() {
            return true;
          }

          @Override
          public Bits bits() {
            return new Bits() {
              @Override
              public boolean get(int index) {
                return bs.fastGet(index + base);
              }

              @Override
              public int length() {
                return maxDoc;
              }
            };
          }

        }, acceptDocs2);
      }
    };
  }

}
