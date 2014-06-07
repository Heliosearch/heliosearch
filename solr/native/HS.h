#ifndef HS_H
#define HS_H

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "org_apache_solr_core_HS.h"

// NOTE: avoid "long"... it is 64 bits on linux/OS-X, but 32 bits on Windows!
// use jlong or int64_t/uint64_t instead.
// Also avoid calling functions that take "long" instead of "long long" or "*int64"

#define HS_FORMAT_BITS org_apache_solr_core_HS_FORMAT_BITS
#define HS_FORMAT_INT8 org_apache_solr_core_HS_FORMAT_INT8 
#define HS_FORMAT_INT16 org_apache_solr_core_HS_FORMAT_INT16 
#define HS_FORMAT_INT32 org_apache_solr_core_HS_FORMAT_INT32 
#define HS_FORMAT_INT64 org_apache_solr_core_HS_FORMAT_INT64 

#define ctz64(val) __builtin_ctzll(val)

#endif // HS_H

