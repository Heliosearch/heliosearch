package org.apache.solr.core;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class HS
{
  /** format codes to pass to native code to describe the data structure */
  public static final int FORMAT_BITS      = 1;
  public static final int FORMAT_INT8      = 2;
  public static final int FORMAT_INT16     = 3;
  public static final int FORMAT_INT32     = 4;
  public static final int FORMAT_INT64     = 5;
  public static final int FORMAT_MONOTONIC = 6;

  public interface NativeData {
    public long getNativeData();
    public int getNativeFormat();
    public long getNativeSize();
  }

  public static final int BYTE_SIZE   = 1;
  public static final int SHORT_SIZE  = 2;
  public static final int INT_SIZE    = 4;
  public static final int LONG_SIZE   = 8;
  public static final int FLOAT_SIZE  = 4;
  public static final int DOUBLE_SIZE = 8;

  private static Logger log = LoggerFactory.getLogger(HS.class);

  public static boolean loaded = false;
  public static final Unsafe unsafe;

  private static native void _init();

  public static void init() {
    if (loaded) {
      _init();
    }

    // effectively disable max clauses on boolean query
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
  }

  static {

    String OS_Name = System.getProperty("os.name");
    // chop at first space
    int idx = OS_Name.indexOf(' ');
    if (idx > 0) {
      OS_Name = OS_Name.substring(0, idx);
    }

    String libName = "libHS_" + OS_Name + ".so";
    String cwd = System.getProperty("user.dir");
    String solrDir = System.getProperty("solr.dir"); // set by command-line ant tests to use solr/example/native

    try {
      boolean found = false;
      File file;

      if (!found && solrDir != null) {
        file = new File(solrDir + File.separator + "native" + File.separator + libName);
        if (file.exists()) {
          found = true;
          System.load(file.getAbsolutePath());
        }
      }

      if (!found) {
        file = new File(cwd + File.separator + "native" + File.separator + libName);
        if (file.exists()) {
          found = true;
          System.load(file.getAbsolutePath());
        }
      }

      if (!found) {
        // intellij tests CWD will be at the root of lucene/solr, so point it at solr/example/native
        file = new File(cwd + File.separator + "solr" + File.separator + "example" + File.separator + "native" + File.separator + libName);
        if (file.exists()) {
          found = true;
          System.load(file.getAbsolutePath());
        }
      }

      if (!found) {
        // intellij tests CWD will be at the root of lucene/solr, so point it at solr/example/native
        file = new File(cwd + File.separator + "solr" + File.separator + "example" + File.separator + "native" + File.separator + libName);
        if (file.exists()) {
          found = true;
          System.load(file.getAbsolutePath());
        }
      }

      // debugging
      // System.out.println("CWD="+cwd);
      // Properties p = System.getProperties();
      // p.list(System.err);

      if (found) {
        loaded = true;
      }
    } catch (Throwable th) {
      log.error("HS: can't load native library: " + th.getMessage());
      // log.info("HS: can't load native library.", th);  // prevent nasty-looking stack trace for now...
    }

    try {
      Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (sun.misc.Unsafe) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("HS: can't load Unsafe!", e);
    }
  }


  public static class Allocator {
    // buffer pool - guaranteed to be power of two sized so it can be used in hash tables, etc.
    // 8K was picked to be small compared to the typical L1 data cache size of 32K.
    private final long[] bufferList;
    private int numCached = 0;  // number of buffers in the pool
    private long cachedBufferRetrievals;  // number of cache "hits"

    public Allocator() {
      this(1024);
    }

    public Allocator(int bufferPoolSize) {
      bufferList = new long[bufferPoolSize];
    }

    /** Returns a buffer of BUFFER_SIZE_BYTES in bytes.
     *  Scale size according to your element size.
     */
    public long getBuffer() {
      synchronized (bufferList) {
        if (numCached > 0) {
          cachedBufferRetrievals++;
          return bufferList[--numCached];
        }
      }
      return allocator.allocArray(BUFFER_SIZE_BYTES, 1, false);
    }

    public long tryGetBuffer() {
      synchronized (bufferList) {
        if (numCached > 0) {
          cachedBufferRetrievals++;
          return bufferList[--numCached];
        }
      }
      return 0;
    }

    public void releaseBuffer(long buffer) {
      if (!tryAddPool(buffer)) {
        freeArray(buffer);
      }
    }

    public boolean tryAddPool(long buffer) {
      assert arraySizeBytes(buffer) == BUFFER_SIZE_BYTES;
      synchronized (bufferList) {
        if (numCached < bufferList.length) {
          bufferList[numCached++] = buffer;
          return true;
        }
      }
      return false;
    }

    public void clearBufferPool() {
      synchronized (bufferList) {
        while (numCached > 0) {
          allocator.doFree( bufferList[--numCached] );
        }
      }
    }

    public long getCachedBufferRetrievals() {
      return cachedBufferRetrievals;
    }


    public long allocArray(long numElements, int elementSize, boolean zero) throws OutOfMemoryError {
      // any JVM accounting for memory allocated this way?
      long sz = numElements * elementSize;

      // try to use buffer pool
      if (sz == BUFFER_SIZE_BYTES) {
        long ret = tryGetBuffer();
        if (ret != 0) {
          if (zero) {
            unsafe.setMemory(ret, sz, (byte)0);
          }
          return ret;
        }
      }

      long addr = unsafe.allocateMemory(sz + HEADER_SIZE);

      numAlloc.incrementAndGet();

      if (zero) {
        // zero all the memory, including the header
        unsafe.setMemory(addr, sz + HEADER_SIZE, (byte)0);
      }

      // should never be 0 since we always add a header
      addr += HEADER_SIZE;
      unsafe.putLong(addr - SIZE_OFFSET, sz);

      return addr;
    }

    public void freeArray(long ptr) {
      long sz = arraySizeBytes(ptr);
      assert sz >= 0;
      if (sz == BUFFER_SIZE_BYTES && tryAddPool(ptr)) {
        return;
      }
      doFree(ptr);
    }

    private void doFree(long ptr) {
      numFree.incrementAndGet();
      unsafe.putLong(ptr - SIZE_OFFSET, -123456789L);  // put negative length to trip asserts
      unsafe.freeMemory(ptr - HEADER_SIZE);
    }

    public void reset() {
      // TODO - reset numAlloc and numFree here?
    }

    public void debug() {

    }
  }

  // An allocator for debugging that tracks every allocation
  public static class TrackingAllocator extends Allocator {
    private static class Info {
      long ptr;
      StackTraceElement[] stack;
    }

    Map<Long, Info> map = new LinkedHashMap<>();

    public TrackingAllocator() {
      super(0); // effectively disable buffer pool to ease debugging
    }

    @Override
    public long allocArray(long numElements, int elementSize, boolean zero) throws OutOfMemoryError {
      Info info = new Info();
      Thread thread = Thread.currentThread();
      info.stack = thread.getStackTrace();
      // TODO: would storing String take up less space?
      // ManagementFactory.getThreadMXBean().getThreadInfo(Thread.currentThread().getId());

      synchronized (this) {
        long ptr = super.allocArray(numElements, elementSize, zero);

        info.ptr = ptr;
        Info prev = map.put(ptr, info);

        if (prev != null) {
          throw new RuntimeException("HS Allocator ERROR : should be impossible!!!");
        }

        return ptr;
      }
    }

    @Override
    public void freeArray(long ptr) {
      synchronized (this) {
        Info curr = map.remove(ptr);

        if (curr == null) {
          // TODO: if we provide a way to clear, it's not an error.
          // TODO: will it be possible to dynamically switch to a debugging allocator?  would be good for debugging live customer site...
          // perhaps detect if we have been switched on and not consider this an error...
          throw new RuntimeException("HS Allocator ERROR: no record of " + ptr + " , bad pointer or double free.");
        }

        super.freeArray(ptr);
      }
    }

    @Override
    public void debug() {
      synchronized (this) {
        int n = map.size();
        int show = Math.min(n, 20);
        if (n > 0) {
          log.error("CURRENT ALLOCATIONS=" + n + (n==show ? "" : " Showing first " + show));
        }
        for (Info info : map.values()) {
          log.error("PTR: " + info.ptr + " SIZE=" + arraySizeBytes(info.ptr) + " ALLOCATED AT " + Diagnostics.toString(info.stack, HS.class.getSimpleName()));
        }

      }
    }
  }

  public static Allocator allocator = new Allocator();
  // public static Allocator allocator = new TrackingAllocator();

  private static final AtomicLong numAlloc = new AtomicLong();
  private static final AtomicLong numFree = new AtomicLong();

  public static final int HEADER_SIZE = 16;
  public static final int SIZE_OFFSET = 8;

  public static long getNumAllocations() {
    return numAlloc.get();
  }

  public static long getNumFrees() {
    return numFree.get();
  }


  // buffer pool - guaranteed to be power of two sized so it can be used in hash tables, etc.
  // 8K was picked to be small compared to the typical L1 data cache size of 32K.
  public static final int BUFFER_SIZE_BITS = 13;
  public static final int BUFFER_SIZE_BYTES = 1<<BUFFER_SIZE_BITS;

  public static long getBuffer() {
    return allocator.getBuffer();
  }

  public static long allocArray(long numElements, int elementSize, boolean zero) throws OutOfMemoryError {
    return allocator.allocArray(numElements, elementSize, zero);
  }

  public static void freeArray(long ptr) {
    allocator.freeArray(ptr);
  }

  public static long arraySizeBytes(long ptr) {
    assert ptr >= 4095 && unsafe.getLong(ptr - SIZE_OFFSET) >= 0;     // if this assertion trips, it's most likely because of a double free
    long sz = unsafe.getLong(ptr - SIZE_OFFSET);
    return sz;
  }

  public static byte getByte(long ptr, int index) {
    assert (index>=0) && ((((long)index+1))) <= arraySizeBytes(ptr);
    return unsafe.getByte(ptr + (((long) index)));
  }
  public static byte getByte(long ptr, long index) {
    assert (index>=0) && ((((long)index+1))) <= arraySizeBytes(ptr);
    return unsafe.getByte(ptr + (((long) index)));
  }


  public static void setByte(long ptr, int index, byte val) {
    assert (index>=0) && ((((long)index+1))) <= arraySizeBytes(ptr);
    unsafe.putByte(ptr + (((long)index)), val);
  }
  public static void setByte(long ptr, long index, byte val) {
    assert (index>=0) && ((((long)index+1))) <= arraySizeBytes(ptr);
    unsafe.putByte(ptr + (((long)index)), val);
  }



  public static int getShort(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<1)) <= arraySizeBytes(ptr);
    return unsafe.getShort(ptr + (((long)index)<<1));
  }

  public static void setShort(long ptr, int index, short val) {
    assert (index>=0) && ((((long)index+1)<<1)) <= arraySizeBytes(ptr);
    unsafe.putShort(ptr + (((long)index)<<1), val);
  }

  public static int getInt(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    return unsafe.getInt(ptr + (((long)index)<<2));
  }

  public static void setInt(long ptr, int index, int val) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    unsafe.putInt(ptr + (((long)index)<<2), val);
  }

  public static void incInt(long ptr, int index, int increment) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    long elemPointer = ptr + (((long)index)<<2);
    int val = unsafe.getInt(elemPointer);
    unsafe.putInt(elemPointer, val + increment);
  }

  public static float getFloat(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    return unsafe.getFloat(ptr + (((long) index) << 2));
  }

  public static void setFloat(long ptr, int index, float val) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    unsafe.putFloat(ptr + (((long) index) << 2), val);
  }

  public static long getLong(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    return unsafe.getLong(ptr + (((long) index) << 3));
  }

  public static void setLong(long ptr, int index, long val) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    unsafe.putLong(ptr + (((long) index) << 3), val);
  }

  /** ptr[index] |= val */
  public static void setLongOR(long ptr, int index, long val) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    long x = ptr + (((long) index) << 3);
    unsafe.putLong(x, unsafe.getLong(x) | val);
  }

  /** ptr[index] &= val */
  public static void setLongAND(long ptr, int index, long val) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    long x = ptr + (((long) index) << 3);
    unsafe.putLong(x, unsafe.getLong(x) & val);
  }

  /** ptr[index] ^= val */
  public static void setLongXOR(long ptr, int index, long val) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    long x = ptr + (((long) index) << 3);
    unsafe.putLong(x, unsafe.getLong(x) ^ val);
  }

  public static double getDouble(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    return unsafe.getDouble(ptr + (((long) index) << 3));
  }

  public static void setDouble(long ptr, int index, double val) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    unsafe.putDouble(ptr + (((long) index) << 3), val);
  }

  public static void copyLengthPrefixBytes(long bytesArr, long offset, BytesRef target) {
    assert bytesArr>=0 && offset>=0 && offset+2 <= arraySizeBytes(bytesArr);
    copyLengthPrefixBytes(bytesArr + offset, target);
  }

  /* WARNING: this method can't verify the pointer */
  public static void copyLengthPrefixBytes(long pointer, BytesRef target) {
    int len = unsafe.getByte(pointer++);
    if (len < 0) {
      len = ((len & 0x7f) << 8) | (unsafe.getByte(pointer++) & 0xff);
    }
    if (target.offset + len >= target.bytes.length) {
      target.bytes = new byte[len];
      target.offset = 0;
    }
    target.length = len;
    unsafe.copyMemory(null, pointer,  target.bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + target.offset, len);

  }

  /* WARNING: this method can't verify the pointer2 */
  public static int compareLengthPrefixBytes(long pointer1, long pointer2) {
    int len = unsafe.getByte(pointer1++);
    if (len < 0) {
      len = ((len & 0x7f) << 8) | (unsafe.getByte(pointer1++) & 0xff);
    }
    return compareLengthPrefixBytes(len, pointer1, pointer2);
  }

  /* termPointerBytes points directly to the start of the bytes of the first term (and len1 contains
   * the length of that term.  termPointer2 points to a normal length prefixed term.  This avoids
   * decoding the length of the first term multiple times when doing a binary search for example.
   * WARNING: this method can't verify the pointers.
   */
  public static int compareLengthPrefixBytes(int len1, long termPointerBytes, long termPointer2) {
    assert len1 >= 0 && len1 < 0x7fff && termPointerBytes > 0 && termPointer2 > 0;
    int len2 = unsafe.getByte(termPointer2++);
    if (len2 < 0) {
      len2 = ((len2 & 0x7f) << 8) | (unsafe.getByte(termPointer2++) & 0xff);
    }

    int upTo = len1 < len2 ? len1 : len2;
    for (int i=0; i<upTo; i++) {
      int diff = (unsafe.getByte(termPointerBytes + i) & 0xff) - (unsafe.getByte(termPointer2 + i) & 0xff);  // compare unsigned
      if (diff != 0) return diff;
    }

    return len1 - len2;
  }


  public static int compareLengthPrefixBytes(long termPointer, BytesRef termPointer2) {
    assert termPointer >= 0;
    int len = unsafe.getByte(termPointer++);
    if (len < 0) {
      len = ((len & 0x7f) << 8) | (unsafe.getByte(termPointer++) & 0xff);
    }

    byte[] arr2 = termPointer2.bytes;
    int offset2 = termPointer2.offset;
    int len2 = termPointer2.length;

    int upTo = len < len2 ? len : len2;
    for (int i=0; i<upTo; i++) {
      int diff = (unsafe.getByte(termPointer + i) & 0xff) - (arr2[offset2+i] & 0xff);  // compare unsigned
      if (diff != 0) return diff;
    }

    return len - len2;
  }

  public static int getTermLength(long termPointer) {
    int len = unsafe.getByte(termPointer);
    if (len < 0) {
      len = ((len & 0x7f) << 8) | (unsafe.getByte(termPointer + 1) & 0xff);
    }
    return len;
  }


  public static void copyBytes(byte[] srcArray, int srcOff, long targetPointer, long targetOff,  int numElements) {
    long nbytes = ((long)numElements);
    assert srcOff>=0 && targetOff>=0 && (targetOff + nbytes) <= arraySizeBytes(targetPointer);
    unsafe.copyMemory(srcArray, Unsafe.ARRAY_BYTE_BASE_OFFSET + (((long)srcOff)), null, targetPointer+targetOff, nbytes);
  }

  public static void copyBytes(long srcPointer, long srcOff, byte[] target, int targetOff,  int numBytes) {
    assert srcOff>=0 && targetOff>=0 && (targetOff + numBytes) <= target.length && (srcOff + numBytes) <= arraySizeBytes(srcPointer);
    unsafe.copyMemory(null, srcPointer + srcOff, target, Unsafe.ARRAY_BYTE_BASE_OFFSET + targetOff, numBytes);
  }

  public static void copyBytes(long srcPointer, long srcOff, long targetPointer, long targetOff,  long numBytes) {
    assert srcOff>=0 && targetOff>=0 && (targetOff + numBytes) <= arraySizeBytes(targetPointer) && (srcOff + numBytes) <= arraySizeBytes(srcPointer);
    unsafe.copyMemory(srcPointer + srcOff, targetPointer + targetOff, numBytes);
  }

  public static void copyInts(int[] srcArray, int srcOff, long targetPointer, long targetOff,  int numElements) {
    long targetOffBytes = targetOff<<2;
    long nbytes = ((long)numElements) << 2;
    assert srcOff>=0 && targetOff>=0 && (targetOffBytes + nbytes) <= arraySizeBytes(targetPointer);
    unsafe.copyMemory(srcArray, Unsafe.ARRAY_INT_BASE_OFFSET + (((long)srcOff)<<2), null, targetPointer+targetOffBytes, nbytes);
  }

  public static void copyLongs(long[] srcArray, int srcOff, long targetPointer, long targetOff,  int numElements) {
    long targetOffBytes = targetOff<<3;
    long nbytes = ((long)numElements) << 3;
    assert srcOff>=0 && targetOff>=0 && (targetOffBytes + nbytes) <= arraySizeBytes(targetPointer);
    unsafe.copyMemory(srcArray, Unsafe.ARRAY_LONG_BASE_OFFSET + (((long)srcOff)<<3), null, targetPointer+targetOffBytes, nbytes);
  }

  public static void copyInts(long sourcePointer, long srcOff, int[] targetArray, int targetOff, int numElements) {
    long srcOffBytes = srcOff<<2;
    long nbytes = ((long)numElements) << 2;
    assert srcOff>=0 && targetOff>=0 && (srcOffBytes + nbytes) <= arraySizeBytes(sourcePointer) && (targetOff+numElements<=targetArray.length);
    unsafe.copyMemory(null, sourcePointer+srcOffBytes, targetArray, Unsafe.ARRAY_INT_BASE_OFFSET + (((long)targetOff)<<2), nbytes);
  }

  public static void copyLongs(long sourcePointer, long srcOff, long[] targetArray, int targetOff, int numElements) {
    long srcOffBytes = srcOff<<3;
    long nbytes = ((long)numElements) << 3;
    assert srcOff>=0 && targetOff>=0 && (srcOffBytes + nbytes) <= arraySizeBytes(sourcePointer) && (targetOff+numElements<=targetArray.length);
    unsafe.copyMemory(null, sourcePointer+srcOffBytes, targetArray, Unsafe.ARRAY_LONG_BASE_OFFSET + (((long)targetOff)<<3), nbytes);
  }

  public static void copyInts(long src, long srcOff, long dest, long destOff, long len) {
    long srcOffBytes = srcOff<<2;
    long destOffBytes = destOff<<2;
    long nBytes = len<<2;
    assert(srcOff>=0 && destOff>=0 && srcOffBytes+nBytes <= arraySizeBytes(src) && destOffBytes+nBytes <= arraySizeBytes(dest));
    unsafe.copyMemory(src+srcOffBytes, dest+destOffBytes, nBytes);
  }

  public static void copyLongs(long src, long srcOff, long dest, long destOff, long len) {
    long srcOffBytes = srcOff<<3;
    long destOffBytes = destOff<<3;
    long nBytes = len<<3;
    assert(srcOff>=0 && destOff>=0 && srcOffBytes+nBytes <= arraySizeBytes(src) && destOffBytes+nBytes <= arraySizeBytes(dest));
    unsafe.copyMemory(src+srcOffBytes, dest+destOffBytes, nBytes);
  }

  //////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////

  /** Sorts a list of integers.  The maximum possible value an integer in the list is passed as a hint.
   * maxPossible may be larger than any values in the array, but should not be smaller.
   * For sorting docids, simply pass maxDoc.
   */
  public static native int sortDedupInts(long intPointer, int numInts, int maxPossible);




  public static String hex(long arr, long offset) {
    StringBuilder sb = new StringBuilder();
    long len = HS.arraySizeBytes(arr);
    sb.append("arr=" + arr + " sizeInBytes=" + len + " offset=" + offset);
    int nBytes = (int)Math.min(16, len - offset);
    return sb.toString() + hexBytes(arr+offset, nBytes);
  }

  public static String hexBytes(long mem, int nBytes) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<nBytes; i++) {
      int b = unsafe.getByte(mem + i) & 0xff;
      if ((i & 0x03)==0) sb.append(' ');
      if (b < 0x10) sb.append('0');
      sb.append(Integer.toHexString(b));
    }
    return sb.toString();
  }

  // TODO: introduce the concept of a reference list to ease deallocation?  (just an auto-expanding long[])

  // TODO: YCS: a finalizer to clean up native memory?
}
