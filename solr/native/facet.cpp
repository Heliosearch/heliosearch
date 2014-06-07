#include "docset.h"
#include "org_apache_solr_search_facet_SimpleFacets.h"

using namespace std;





template <typename DS, typename NumericPtr>
void fillCounts(DS& docs, NumericPtr ords, int numTermsInField, int startTermIndex, int endTermIndex, int* counts) {
  // external "user" ords in lucene/solr have 0 as the first real ord, with  -1 for missing...
  // but internally they are stored from 1..maxOrd inclusive, with 0 meaning missing and 1 is subtracted before passing to the user.
  // hence the "+1" to startTermIndex here...
  int adjustment = startTermIndex + 1;
  int nTerms = endTermIndex - startTermIndex;
  typename DS::iterator docIter = docs.begin();
  typename DS::iterator end = docs.end();

  if (nTerms == numTermsInField+1) { 
    // we're collection the full range (including missing)
    // assert (adjustment == 0);
    // fprintf(stdout, "FAST MODE!!!!!!!!!!!!!!!!!!!!!!!\n"); fflush(stdout);
    for (; docIter != end; ++docIter) {
      int term = ords[*docIter];
      counts[term]++;
    }
  } else {
    for (; docIter != end; ++docIter) {
      int term = ords[*docIter];
      int arrIdx = term - adjustment;
      if (arrIdx >= 0 && arrIdx < nTerms) {
        counts[arrIdx]++;
      }
    }
  }
}

template <typename DS>
void fillCounts(DS& docs, jlong ordArr, int ordFormat, jlong ordSize, int numTermsInField,
    int startTermIndex, int endTermIndex, int* counts) {

  switch (ordFormat) {
    case HS_FORMAT_INT8:
      fillCounts(docs, (uint8_t*)ordArr, numTermsInField, startTermIndex, endTermIndex, counts);
      break;
    case HS_FORMAT_INT16:
      fillCounts(docs, (uint16_t*)ordArr, numTermsInField, startTermIndex, endTermIndex, counts);
      break;
    case HS_FORMAT_INT32:
      fillCounts(docs, (uint32_t*)ordArr, numTermsInField, startTermIndex, endTermIndex, counts);
      break;
    case HS_FORMAT_INT64:
      fillCounts(docs, (uint64_t*)ordArr, numTermsInField, startTermIndex, endTermIndex, counts);
      break;
    default:
      //log("unknown format"); // TODO
      break;
  }
} 

//  private static native void fillCounts(long baseArr, int baseFormat, long baseSize, long ordArr, int ordFormat, long ordSize, int startTermIndex, int endTermIndex, int offset, int limit, long counts);

JNIEXPORT void JNICALL Java_org_apache_solr_search_facet_SimpleFacets_fillCounts
  (JNIEnv *env, jclass clazz,
   jlong baseArr, jint baseFormat, jlong baseSize,
   jlong ordArr, jint ordFormat, jlong ordSize, jint numTermsInField,
   jint startTermIndex, jint endTermIndex,
   jint offset, jint limit, jlong counts)
{
  if (ordArr == 0 || baseArr == 0 || counts == 0) {
    return;
  }

  switch (baseFormat) {
    case HS_FORMAT_BITS: 
      {
      BitDocSet bitDocs = BitDocSet((uint64_t*)baseArr, (int)baseSize); 
      fillCounts(bitDocs, ordArr, ordFormat, ordSize, numTermsInField, startTermIndex, endTermIndex, (int*)counts);
      break;
      }
    case HS_FORMAT_INT32:
      {
      SortedIntDocSet intDocs = SortedIntDocSet((int*)baseArr, (int)baseSize); 
      fillCounts(intDocs, ordArr, ordFormat, ordSize, numTermsInField, startTermIndex, endTermIndex, (int*)counts);
      break;
      }
    default:
      // log error
      break;
  }
}



