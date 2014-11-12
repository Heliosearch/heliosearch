/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.sindicetech.siren.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/**
 * Contains some utility methods for manipulating arrays.
 */
public class ArrayUtils {

  /**
   * Copy the reference of the underlying array with the associated offset and length.
   */
  public static final void shallowCopy(IntsRef source, IntsRef target) {
    target.ints = source.ints;
    target.offset = source.offset;
    target.length = source.length;
  }

  /**
   * Increase the size of the array if needed. Copy the original content into
   * the new one and update the int[] reference inside the IntsRef.
   */
  public static final void growAndCopy(final IntsRef ref, final int size) {
    final int[] newArray = growAndCopy(ref.ints, size);
    ref.ints = newArray;
  }

  /**
   * Increase the size of the array and copy the content of the original array
   * into the new one.
   */
  public static final int[] growAndCopy(final int[] array, final int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      final int[] newArray = new int[minSize];
      System.arraycopy(array, 0, newArray, 0, array.length);
      return newArray;
    } else {
      return array;
    }
  }

  /**
   * Increase the size of the array if needed. Do not copy the content of the
   * original array into the new one.
   * <p>
   * Do not over allocate.
   */
  public static final int[] grow(final int[] array, final int size) {
    assert size >= 0: "size must be positive (got " + size + "): likely integer overflow?";
    if (array.length < size) {
      final int[] newArray = new int[size];
      return newArray;
    } else {
      return array;
    }
  }

  /**
   * Increase the size of the array if needed. Do not copy the content of the
   * original array into the new one.
   */
  public static final IntsRef grow(final IntsRef ref, final int minSize) {
    ref.ints = grow(ref.ints, minSize);
    return ref;
  }

  /**
   * Increase the size of the array if needed. Do not copy the content of the
   * original array into the new one.
   */
  public static final byte[] grow(final byte[] array, final int minSize) {
    assert minSize >= 0: "size must be positive (got " + minSize + "): likely integer overflow?";
    if (array.length < minSize) {
      final byte[] newArray = new byte[minSize];
      return newArray;
    } else {
      return array;
    }
  }

  /**
   * Increase the size of the array if needed. Do not copy the content of the
   * original array into the new one.
   */
  public static final BytesRef grow(final BytesRef ref, final int minSize) {
    ref.bytes = grow(ref.bytes, minSize);
    return ref;
  }

}
