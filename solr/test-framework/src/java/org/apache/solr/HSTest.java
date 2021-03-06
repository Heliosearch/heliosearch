package org.apache.solr;

import junit.framework.TestCase;
import org.apache.solr.core.HS;
import org.apache.solr.search.DocSetBaseNative;
import org.slf4j.Logger;

public class HSTest {
  public static Logger log = SolrTestCaseJ4.log;


  public static void startTracking() {
    startTrackingMemory();
  }


  public static void endTracking() {
    endTrackingMemory();
  }


  public static long startAllocations;
  public static long startFrees;
  public static long startBufferRetrievals;

  public static void startTrackingMemory() {
    assert HS.loaded;
    startAllocations = HS.getNumAllocations();
    startFrees = HS.getNumFrees();
    startBufferRetrievals = HS.allocator.getCachedBufferRetrievals();
  }

  public static void endTrackingMemory() {
    HS.allocator.clearBufferPool();

    DocSetBaseNative.debug(true);

    HS.allocator.debug();

    long endAllocations = HS.getNumAllocations();
    long endFrees = HS.getNumFrees();

    long numAllocations = endAllocations - startAllocations;
    long numFrees = endFrees - startFrees;
    long numLeaks = numAllocations - numFrees;
    long numBufferRetrievals = HS.allocator.getCachedBufferRetrievals() - startBufferRetrievals;

    log.info("numAllocations="+numAllocations + " cachedBufferRetrievals="+numBufferRetrievals);
    if (numLeaks != 0) {
      String msg = "HS ERROR: MEMORY LEAKS = " + numLeaks;
      log.error(msg);
      TestCase.fail(msg);
    }
  }
}
