// #include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "org_apache_solr_core_HS.h"

using namespace std;

JNIEXPORT void JNICALL Java_org_apache_solr_core_HS__1init(JNIEnv *, jclass) {
  fprintf(stdout, "!!!!!! Heliosearch native libraries loaded. !!!!!!\n");
  fflush(stdout);
}

