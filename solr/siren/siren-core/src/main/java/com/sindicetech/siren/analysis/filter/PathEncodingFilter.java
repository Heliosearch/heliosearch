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

package com.sindicetech.siren.analysis.filter;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import com.sindicetech.siren.analysis.attributes.PathAttribute;

import java.io.IOException;

/**
 * Filter that encodes the path found in {@link com.sindicetech.siren.analysis.attributes.PathAttribute} by
 * prepending it to the value of the token.
 */
public class PathEncodingFilter extends TokenFilter {

  private final CharTermAttribute termAtt;
  private final PathAttribute pathAtt;
  private final PositionIncrementAttribute posIncrAtt;

  public static boolean DEFAULT_PRESERVE_ORIGINAL = false;
  private boolean preserveOriginal = DEFAULT_PRESERVE_ORIGINAL;
  private boolean hasOriginalTokenPending = false;
  private boolean hasTokenPending = false;

  public static final char PATH_DELIMITER = ':';

  public PathEncodingFilter(final TokenStream input) {
    super(input);
    termAtt = this.addAttribute(CharTermAttribute.class);
    pathAtt = this.addAttribute(PathAttribute.class);
    posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);
  }

  /**
   * Default to false. If true, the original token is preserved, i.e., the filter will output two tokens, the original
   * one and the one with the path prepended.
   */
  public void setPreserveOriginal(boolean preserveOriginal) {
    this.preserveOriginal = preserveOriginal;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (!hasTokenPending && !hasOriginalTokenPending) {
      if (!input.incrementToken()) {
        return false;
      }
      hasTokenPending = true;
      hasOriginalTokenPending = preserveOriginal ? true : false;
    }

    // returns the original token
    if (hasOriginalTokenPending) {
      hasOriginalTokenPending = false;
      if (termAtt.length() != 0) { // only return non-empty tokens
        return true;
      }
    }

    boolean isTermEmpty = termAtt.length() == 0;
    String field = pathAtt.field();
    int newLength = field.length() + 1 + termAtt.length();
    termAtt.resizeBuffer(newLength);
    char[] termBuffer = termAtt.buffer();
    // shift term to the end of the buffer
    System.arraycopy(termBuffer, 0, termBuffer, field.length() + 1, termAtt.length());
    // prepend the field name and delimiter
    field.getChars(0, field.length(), termBuffer, 0);
    termBuffer[field.length()] = PATH_DELIMITER;
    // update length of the term
    termAtt.setLength(newLength);
    // set position increment to 0 only if the original token has been returned
    if (preserveOriginal) {
      // if the original term was empty, leave the position increment to 1
      posIncrAtt.setPositionIncrement(isTermEmpty ? 1 : 0);
    }

    hasTokenPending = false;
    return true;
  }

}
