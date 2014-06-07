// #include <iostream>
#include <stdio.h>
#include <stdlib.h>
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

JNIEXPORT void JNICALL Java_org_apache_solr_core_HS__1init(JNIEnv *, jclass) {
  fprintf(stdout, "!!!!!! Heliosearch native libraries loaded. !!!!!!\n");
  fflush(stdout);
  checkDebug();
}

