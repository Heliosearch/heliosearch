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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.util.Version;

import com.sindicetech.siren.analysis.TupleTokenizer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Filter out tokens with a given type.
 */
@Deprecated
public class TokenTypeFilter extends FilteringTokenFilter {

  protected Set<String> stopTokenTypes;

  private final TypeAttribute typeAtt = this.addAttribute(TypeAttribute.class);

  public TokenTypeFilter(final Version version, final TokenStream input, final int[] stopTokenTypes) {
    super(version, true, input);
    this.stopTokenTypes = new HashSet<String>(stopTokenTypes.length);
    for (final int type : stopTokenTypes) {
      this.stopTokenTypes.add(TupleTokenizer.getTokenTypes()[type]);
    }
  }

  @Override
  protected boolean accept() throws IOException {
    if (stopTokenTypes.contains(typeAtt.type())) {
      return false;
    }
    return true;
  }



}
