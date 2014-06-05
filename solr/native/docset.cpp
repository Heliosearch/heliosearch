#include "docset.h"

using namespace std;

JNIEXPORT jint JNICALL Java_org_apache_solr_search_SortedIntDocSetNative_intersectionSizeNative
  (JNIEnv *env, jclass clazz, jlong a, jint a_size, jlong b, jint b_size)
{
  return intersectionSize( (const int*)a, a_size, (const int*)b, b_size );
}


int intersectionSize(const int* const a, int32_t a_size, const int* const b, int32_t b_size) {
    // The next doc we are looking for will be much closer to the last position we tried
    // than it will be to the midpoint between last and high... so probe ahead using
    // a function of the ratio of the sizes of the sets.
    uint32_t step = (b_size/a_size)+1;

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


    // printf("a=%p sz=%d b=%p sz=%d step=%d\n", (void*)a, a_size, (void*)b, b_size, step);

    int icount = 0;
    int low = 0;
    int max = b_size-1;

    for (int i=0; i<a_size; i++) {
      int doca = a[i];

      int high = max;  // note: these *must* be signed types (since high can end up at -1)

      int probe = low + step;     // 40% improvement!

      // short linear probe to see if we can drop the high pointer in one big jump.
      if (probe<high) {
        if (b[probe]>=doca) {
          // success!  we cut down the upper bound by a lot in one step!
          high=probe;
        } else {
          // relative failure... we get to move the low pointer, but not my much
          low=probe+1;

          // reprobe worth it? it appears so!
          probe = low + step;
          if (probe<high) {
            if (b[probe]>=doca) {
              high=probe;
            } else {
              low=probe+1;
            }
          }
        }
      }

      // binary search the rest of the way
      while (low <= high) {
        int mid = ((unsigned int)(low+high)) >> 1;  // make sure this is an unsigned shift to handle overflow
        int docb = b[mid];

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



