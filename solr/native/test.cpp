#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "docset.h"

using namespace std;

uint64_t _rlong = 0x1234567887654321;  // anything but 0 works...
uint64_t randomLong() {
  _rlong ^= (_rlong << 21);
  _rlong ^= (_rlong >> 35);
  _rlong ^= (_rlong << 4);
  return _rlong;
}


uint64_t* getBits(int words) {
  uint64_t* arr = new uint64_t[words];
  for (int i=0; i<words; i++) {
    arr[i] = randomLong();
  }
  return arr;
}

BitDocSet getBitDocSet(int words) {
  uint64_t* arr = getBits(words);
  return BitDocSet(arr, words);
}


template <typename DS>
int count(DS set, int& docsum) {
  int ret=0;
  int docs=0;
  for (typename DS::iterator iter = set.begin(); iter != set.end(); ++iter) {
    docs += *iter;
    // printf("doc=%d\n", doc);
    ret++;
  }
  docsum+= docs;
  return ret;
}

// to see generated asm
int countBS(BitDocSet set, int& docsum) {
  return count(set, docsum);
}

void testIter() {
  int docsum;
  BitDocSet set = getBitDocSet(1);
  *(jlong*)set.bits = 0;  // test the 0 case
  assert( count(set, docsum) == 0 );
  delete set.bits;

  set = getBitDocSet(2);
  *(jlong*)(set.bits+1) = 0;  // test the 0 case at end
  assert( count(set, docsum) > 0 );
  delete set.bits;

  set = getBitDocSet(2);
  *(jlong*)set.bits = 0;  // test the 0 case at start
  assert( count(set, docsum) > 0 );
  delete set.bits;

  jlong ret = 0;
  for (int i=0; i<1000000; i++) {
    int len = randomLong() & 0x03; 
    set = getBitDocSet(len);
    ret += count(set, docsum);
  }

}



int main(int argc, char** argv) {
  testIter();
}


