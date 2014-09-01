#include "HS.h"
#include "org_apache_solr_search_SortedIntDocSetNative.h"
#include "org_apache_solr_search_BitDocSetNative.h"


int intersectionSize(const int* const a, int32_t a_size, const int* const b, int32_t b_size);


class DocSet {
};

class BitSetIterator;

class BitDocSet : public DocSet {
  public:
    uint64_t* bits;
    int wlen;
    // TODO: what about number of bits set?

    typedef BitSetIterator iterator;

    BitDocSet(uint64_t* bits, int wlen) : bits(bits), wlen(wlen) {
    }

    void fastSet(int index) {
      int wordNum = index>>6;
      long bitmask = 1L << (index & 0x3f);
      bits[wordNum] |= bitmask; 
    }

    int nextSetBit(int index) const {
      assert(index >= 0);
      int i = index>>6;
      if (i>=wlen) return -1;
      // gcc will optimize (a<<(x&0x3f)) to (a<<x) on x86.  We should not remove the mask since
      // the C/C++ standards do not guarantee this behavior for x outside 0..63
      uint64_t word = bits[i] >> (index & 0x3f);  // skip all the bits to the right of index

      if (word!=0) {
        // TODO: we should be able to do even better with some inline ASM
        // to eliminate a conditional.  Although since we already check for 0, the
        // branch should be 100% predictable by the CPU.
        return (i<<6) + (index & 0x3f) + ctz64(word);
      }

      while(++i < wlen) {
        word = bits[i];
        if (word!=0) return (i<<6) + ctz64(word);
      }

      return -1;
    }


    iterator begin() const;

    iterator end() const;
};

// TODO: we can probably do a little better with a more stateful iterator
class BitSetIterator {
  public:
    const BitDocSet obs;  // const ref or shallow copy?
    int pos;

    BitSetIterator(const BitDocSet& bitset) : obs(bitset) , pos(-1) {
      ++(*this);  // position on first set bit
    }

    // custom positioning, does not advance to first set bit.  use -1 for "end"
    BitSetIterator(const BitDocSet& bitset, int position) : obs(bitset), pos(position) {
    }

    bool operator==(const BitSetIterator& other) {
      return(pos == other.pos);
    }

    bool operator!=(const BitSetIterator& other) {
      return(pos != other.pos);
    }

    BitSetIterator operator++() {
      pos = obs.nextSetBit(pos+1);
      return *this;  // TODO: ensure this is optimized if not used.. or change return type to void.
    }

    BitSetIterator operator++(int) {
      BitSetIterator tmp(*this); // shallow copy
      ++(*this);
      return(tmp);
    }

    int operator*() {
      return pos;
    }
};


class SortedIntDocSet : public DocSet {
  public:
    const int* docs;
    int len;

    typedef const int* iterator;
    
    SortedIntDocSet(int* docs, int len) : docs(docs), len(len) {
    }

    iterator begin() const {
      return docs;
    }

    iterator end() const {
      return docs + len;
    }
};

inline BitSetIterator BitDocSet::begin() const {
  return BitSetIterator(*this);
}

inline BitSetIterator BitDocSet::end() const {
  return BitSetIterator(*this, -1);
}

