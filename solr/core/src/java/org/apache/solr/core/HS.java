package org.apache.solr.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class HS
{
  private static Logger log = LoggerFactory.getLogger(HS.class);

  public static final Unsafe unsafe;

  private static native void print();

  static {
    try {
      Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (sun.misc.Unsafe) field.get(null);
    } catch (Exception e) {
      throw new RuntimeException("HS: can't load Unsafe!", e);
    }
  }


  public static class Allocator {
    public long allocArray(long numElements, int elementSize, boolean zero) throws OutOfMemoryError {
      // any JVM accounting for memory allocated this way?
      long sz = numElements * elementSize;
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

  public static int getInt(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    return unsafe.getInt(ptr + (((long)index)<<2));
  }

  public static void setInt(long ptr, int index, int val) {
    assert (index>=0) && ((((long)index+1)<<2)) <= arraySizeBytes(ptr);
    unsafe.putInt(ptr + (((long)index)<<2), val);
  }

  public static long getLong(long ptr, int index) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    return unsafe.getLong(ptr + (((long) index) << 3));
  }

  public static void setLong(long ptr, int index, long val) {
    assert (index>=0) && ((((long)index+1)<<3)) <= arraySizeBytes(ptr);
    unsafe.putLong(ptr + (((long) index) << 3), val);
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



  // TODO: introduce the concept of a reference list to ease deallocation?  (just an auto-expanding long[])

  // TODO: YCS: a finalizer to clean up native memory?
}
