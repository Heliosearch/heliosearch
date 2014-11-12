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

import java.io.IOException;
import java.nio.CharBuffer;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Split an URI with a mailto scheme.
 *
 * <p>
 *
 * The mailto URI is tokenised into two tokens:
 * <ul>
 * <li> one token with the 'mailto:' removed (e.g., test@test.fr)
 * <li> one token with the original URI (e.g., mailto:test@test.fr) at the same
 *      position as the last one
 * </ul>
 */
public class MailtoFilter extends TokenFilter {

  private final CharTermAttribute           termAtt;
  private final PositionIncrementAttribute  posIncrAtt;

  private CharBuffer termBuffer;
  private boolean isMailto = false;

  /**
   * @param input
   */
  public MailtoFilter(final TokenStream input) {
    super(input);
    termAtt = this.addAttribute(CharTermAttribute.class);
    posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);
    termBuffer = CharBuffer.allocate(256);
  }

  @Override
  public final boolean incrementToken()
  throws IOException {
    if (isMailto) {
      termAtt.setEmpty();
      // return the scheme + the mail part
      isMailto = false;
      posIncrAtt.setPositionIncrement(0);
      termAtt.copyBuffer(termBuffer.array(), 0, termBuffer.position());
      return true;
    }

    if (input.incrementToken()) {
      if (this.isMailtoScheme()) {
        this.updateBuffer();
        termBuffer.put(termAtt.buffer(), 0, termAtt.length());
        // return only the mail part
        posIncrAtt.setPositionIncrement(1);
        termAtt.copyBuffer(termBuffer.array(), 7, termBuffer.position() - 7);
      }
      return true;
    }
    return false;
  }

  /**
   * Check if the buffer is big enough
   */
  private void updateBuffer() {
    if (termBuffer.capacity() < termAtt.length()) {
      termBuffer = CharBuffer.allocate(termAtt.length());
    }
    termBuffer.clear();
  }

  /**
   *
   * @return true if the URI start with mailto:
   */
  private boolean isMailtoScheme() {
    if (termAtt.length() < 7 || termAtt.charAt(6) != ':') {
      return false;
    }
    if (termAtt.charAt(0) != 'm' || termAtt.charAt(1) != 'a' || termAtt.charAt(2) != 'i' ||
        termAtt.charAt(3) != 'l' || termAtt.charAt(4) != 't' || termAtt.charAt(5) != 'o')
      return false;
    isMailto = true;
    return true;
  }

}
