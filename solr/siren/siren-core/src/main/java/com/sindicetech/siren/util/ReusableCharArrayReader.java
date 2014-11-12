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

import java.io.CharArrayReader;

/**
 * Implementation of the {@link CharArrayReader} that allows to reset the reader
 * to a new char array input.
 */
public class ReusableCharArrayReader extends CharArrayReader {

  /**
   * @param buf
   */
  public ReusableCharArrayReader(final char[] buf) {
    super(buf);
  }

  public ReusableCharArrayReader(final char[] buf, final int offset, final int length) {
    super(buf, offset, length);
  }

  public void reset(final char[] toReset) {
    this.buf = toReset;
    this.pos = 0;
    this.count = toReset.length;
  }

  public void reset(final char[] toReset, final int offset, final int len) {
    this.buf = toReset;
    this.pos = offset;
    this.count = len;
  }

  @Override
  public String toString() {
    return new String(buf, this.pos, this.count);
  }

}
