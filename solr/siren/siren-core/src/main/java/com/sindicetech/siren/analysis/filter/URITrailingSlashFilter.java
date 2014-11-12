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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Filter that removes the trailing slash of the URI token.
 */
public class URITrailingSlashFilter extends TokenFilter {

  private final CharTermAttribute termAtt;

  public URITrailingSlashFilter(final TokenStream in) {
    super(in);
    termAtt = this.addAttribute(CharTermAttribute.class);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }

    final int bufferLength = termAtt.length();
    // Remove trailing slash
    if (termAtt.buffer()[bufferLength - 1] == '/') {
      // Strip last character off
      termAtt.setLength(bufferLength - 1);
    }
    return true;
  }

}
