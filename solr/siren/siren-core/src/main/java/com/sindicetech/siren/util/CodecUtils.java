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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;

/**
 * Contains methods to encode an integer into a byte array and decode a byte
 * array into an integer.
 */
public class CodecUtils {

  /**
   * Convert a byte array to an int (32bit word).
   *
   * @param src The byte array (length = 4)
   * @param offset The offset of the encoded integer in the byte array
   * @return The integer
   */
  public static int byteArrayToInt(final byte[] src, final int offset) {
    int value = 0;
    for (int i = offset; i < offset + 4; i++) {
      value += (src[i] & 0x000000FF) << (4 - 1 - i) * 8;
    }
    return value;
  }

  /**
   * Convert an integer to a byte array.
   *
   * @param x The integer
   * @param dst The byte array (length = 4)
   * @param offset The offset of the encoded integer in the byte array
   */
  public static final void intToByteArray(final int x, final byte[] dst, final int offset) {
    dst[offset + 3] = (byte) (x & 0x000000ff);
    dst[offset + 2] = (byte) ((x & 0x0000ff00) >>> 8);
    dst[offset + 1] = (byte) ((x & 0x00ff0000) >>> 16);
    dst[offset + 0] = (byte) ((x & 0xff000000) >>> 24);
  }

  /**
   * Writes an integer in a variable-length format. Writes between one and
   * five bytes. Smaller values take fewer bytes. Negative numbers are not
   * supported.
   * @see IndexInput#readVInt()
   */
  public static void vIntToByteArray(int i, final ByteBuffer bb) {
    while ((i & ~0x7F) != 0) {
      bb.put((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    bb.put((byte) i);
  }

  /**
   * Reads an int stored in variable-length format. Reads between one and five
   * bytes. Smaller values take fewer bytes. Negative numbers are not supported.
   */
  public static int byteArrayToVInt(final BytesRef ref) {
    byte b = ref.bytes[ref.offset++];
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = ref.bytes[ref.offset++];
      i |= (b & 0x7F) << shift;
    }
    return i;
  }

  /**
   * Writes an integer in a variable-length format. Writes between one and
   * five bytes. Smaller values take fewer bytes. Negative numbers are not
   * supported.
   * @see IndexInput#readVInt()
   */
  public static void appendVInt(int i, final BytesRef ref) {
    while ((i & ~0x7F) != 0) {
      ref.bytes[ref.offset++] = (byte) ((i & 0x7f) | 0x80);
      i >>>= 7;
    }
    ref.bytes[ref.offset++] = (byte) i;
  }

  /**
   * Write the content, i.e. bits, of a byte into the string buffer.
   * </br>
   * Code taken from Lucene-1410.
   *
   * @param b The byte
   * @param buf the string buffer
   */
  private static void writeBits(final int b, final StringBuffer buf) {
    for (int i = 7; i >= 0; i--) {
      buf.append((b >>> i) & 1);
    }
  }

  /**
   * Display the content, i.e. bits, of a byte array. Write to stdout the
   * content of the array on a single line.
   * </br>
   * Code taken from Lucene-1410.
   *
   * @param array The byte array
   */
  public static void writeBytes(final byte[] array) {
    final StringBuffer buf = new StringBuffer();
    for (final byte element : array) {
      CodecUtils.writeBits(element & 255, buf);
      buf.append(' ');
    }
    System.out.println(buf);
  }

  /**
   * Display the content, i.e. bits, of a byte array. Write to stdout 4 bytes
   * per lines.
   * </br>
   * Code taken from Lucene-1410.
   *
   * @param array The byte array
   */
  public static void writeIndentBytes(final byte[] array) {
    final StringBuffer buf = new StringBuffer();
    for (int i = 0; i < array.length; i++) {
      CodecUtils.writeBits(array[i] & 255, buf);
      if (((i+1) % 4) != 0) {
        buf.append(' ');
      } else {
        System.out.println(buf);
        buf.setLength(0);
      }
    }
  }

}
