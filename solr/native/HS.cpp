// #include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include "HS.h"

using namespace std;

void checkDebug() {
  int a=0;
  assert( (a=1, true) );
  if (a==1) {
    const char* msg = "!!!!!! WARNING: Heliosearch native debugging mode enabled. !!!!!!\n";
    fprintf(stdout,msg);
    fprintf(stderr,msg);
    fflush(stdout);
    fflush(stderr);
  }
}

JNIEXPORT void JNICALL Java_org_apache_solr_core_HS__1init(JNIEnv* env, jclass) {
  fprintf(stdout, "!!!!!! Heliosearch native libraries loaded. !!!!!!\n");
  fflush(stdout);
  checkDebug();
}


int dedup2(int* arr, int numInts) {
  if (numInts<=0) return 0;
  int dups = 0;
  int last = arr[0];
  for (int i = 1; i<numInts; i++) {
    int val = arr[i];
    if (val == last) {
      dups++;
    } else {
      last = val;
      arr[i - dups] = val;
    }
  }
  return numInts - dups;
}

// this version starts by assuming there are no dups
// and calls dedup2 at the point the first dup is found.
int dedup(int* arr, int numInts) {
  for (int i = 0; i<(numInts-1); i++) {
    if (arr[i] == arr[i+1]) {
      return i + dedup2(arr+i, numInts-i);
    }
  }
  // no dups
  return numInts;
}


JNIEXPORT jint JNICALL Java_org_apache_solr_core_HS_sortDedupInts(JNIEnv* env, jclass clazz, jlong intPtr, jint numInts, jint maxPossible) {
  int* arr = (int*)intPtr;
  // TODO: try a single pass MSD radix sort first, using maxPossible to try and distribute evenly
  std::sort(arr, arr+numInts);
  return dedup(arr, numInts);
}


