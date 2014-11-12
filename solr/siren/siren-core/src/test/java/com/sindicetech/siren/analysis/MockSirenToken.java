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

import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.analysis.TupleTokenizer;
import com.sindicetech.siren.util.XSDDatatype;

public class MockSirenToken {

  char[] term;
  int startOffset, endOffset;
  int posInc;
  int tokenType;
  char[] datatype;
  IntsRef nodePath;

  private MockSirenToken(final char[] term, final int startOffset,
                         final int endOffset, final int posInc,
                         final int tokenType, final char[] datatype,
                         final IntsRef nodePath) {
    this.term = term;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.posInc = posInc;
    this.tokenType = tokenType;
    this.datatype = datatype;
    this.nodePath = nodePath;
  }

  public static MockSirenToken token(final String term, final IntsRef nodePath) {
    return token(term, 0, 0, 1, TupleTokenizer.LITERAL,
      XSDDatatype.XSD_STRING.toCharArray(), nodePath);
  }

  public static MockSirenToken token(final String term, final IntsRef nodePath, final int posInc) {
    return token(term, 0, 0, posInc, TupleTokenizer.LITERAL,
      XSDDatatype.XSD_STRING.toCharArray(), nodePath);
  }

  public static MockSirenToken token(final String term, final int startOffset,
                                     final int endOffset, final int posInc,
                                     final int tokenType, final char[] datatype,
                                     final IntsRef nodePath) {
    return new MockSirenToken(term.toCharArray(), startOffset, endOffset,
      posInc, tokenType, datatype, nodePath);
  }

  public static IntsRef node(final int ... id) {
    return new IntsRef(id, 0, id.length);
  }

  @Override
  public String toString() {
    return '\'' + new String(term) + '\'';
  }

}
