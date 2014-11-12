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
package com.sindicetech.siren.analysis;

import java.io.IOException;
import java.io.Reader;

public class MockSirenReader extends Reader {

  private final MockSirenDocument doc;

  public MockSirenReader(final MockSirenDocument doc) {
    this.doc = doc;
  }

  public MockSirenDocument getDocument() {
    return doc;
  }

  @Override
  public int read(final char[] cbuf, final int off, final int len) throws IOException {
    return -1;
  }

  @Override
  public void close() throws IOException {}

}
