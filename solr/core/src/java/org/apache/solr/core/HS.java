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
      // zero all the memory, including the header
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
    unsafe.putLong(ptr - SIZE_OFFSET, -123456789L);  // put negative length tp trip asserts
    unsafe.freeMemory(ptr - HEADER_SIZE);
  }

  public static long arraySizeBytes(long ptr) {
    long sz = unsafe.getLong(ptr - SIZE_OFFSET);
    assert ptr >= 4096 && sz >= 0;     // if this assertion trips, it's most likely because of a double free
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
