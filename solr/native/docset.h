#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include "org_apache_solr_search_SortedIntDocSetNative.h"

int intersectionSize(const int* const a, int32_t a_size, const int* const b, int32_t b_size);

/**** work in progress

class DocSet {
};

class BitDocSet : public DocSet {
  public:
    const uint64_t* bits;
    const uint32_t wlen;

    int nextSetBit(int index) const {
      assert(index >= 0);
      int i = index>>6;
      if (i>=wlen) return -1;
      uint32_t subIndex = index & 0x3f;      // index within the word
      uint64_t word = bits[i] >> subIndex;  // skip all the bits to the right of index

      if (word!=0) {
        // TODO: we should be able to do even better with some inline ASM
        // to eliminate a conditional.  Although since we already check for 0, the
        // branch should be 100% predictable by the CPU.
        return (i<<6) + subIndex + ctzl(word);
      }

      while(++i < wlen) {
        word = bits[i];
        if (word!=0) return (i<<6) + ctzl(word);
      }

      return -1;
    }
};

// TODO: we can probably do better with a more stateful iterator
class BitSetIterator {
  public:
    const OpenBitSet obs;  // const ref or shallow copy?
    int pos;

    BitSetIterator(const OpenBitSet& bitset) : obs(bitset) {
      ++(*this);  // position on first set bit
    }

    // custom positioning, use -1 for "end"
    BitSetIterator(const OpenBitSet& bitset, int position) : obs(bitset), pos(position) {
      ++(*this);  // position on first set bit
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

***/

