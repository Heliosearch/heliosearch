package org.apache.solr.core;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class HS
{
  public static boolean loaded = false;
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
    numAlloc.incrementAndGet();

    // zero array?
    // any JVM accounting for memory allocated this way?
    long sz = numElements * elementSize;
    long addr = unsafe.allocateMemory(sz + HEADER_SIZE);

    if (zero) {
      unsafe.setMemory(addr, sz + HEADER_SIZE, (byte)0);
    }

    // should never be 0 since we always add a header
    addr += HEADER_SIZE;
    // TODO: add to a set to keep track of native memory?
    unsafe.putLong(addr - SIZE_OFFSET, sz);
    return addr;
  }

  public static void freeArray(long ptr) {
    numFree.incrementAndGet();

    // zero out length to trip asserts that try to use the memory after this point
    unsafe.putLong(ptr - SIZE_OFFSET, 0);
    unsafe.freeMemory(ptr - HEADER_SIZE);
  }

  public static long arraySizeBytes(long ptr) {
    assert(ptr>=4096); // should never be on first page, or negative
    return unsafe.getLong(ptr - SIZE_OFFSET);
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

  public static void copyToPointer(long targetPointer, long targetPos, int[] srcArray, int srcPos, int srcLength) {
    long start = targetPos<<2;
    long nbytes = ((long)srcLength) << 2;
    assert start + nbytes <= arraySizeBytes(targetPointer);
    unsafe.copyMemory(srcArray, Unsafe.ARRAY_INT_BASE_OFFSET + (srcPos<<2), null, targetPointer+start, nbytes);
  }

  public static void copyInts(long src, long srcOff, long len, long dest, long destOff) {
    long srcStart = src + (srcOff<<2);
    long destStart = dest + (destOff<<2);
    long nBytes = len << 2;
    assert(srcStart+nBytes <= arraySizeBytes(src) && destStart+nBytes <= arraySizeBytes(dest));
    unsafe.copyMemory(srcStart, destStart, nBytes);
  }


  // TODO: YCS: a finalizer to clean up native memory?
}
