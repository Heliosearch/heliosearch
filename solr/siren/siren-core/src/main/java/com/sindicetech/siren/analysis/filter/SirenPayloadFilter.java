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
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.PositionAttribute;

/**
 * Filter that encodes the {@link NodeAttribute} and the
 * {@link PositionAttribute} into the {@link PayloadAttribute}.
 */
public class SirenPayloadFilter extends TokenFilter  {

  private final NodeAttribute nodeAtt;
  private final PositionAttribute posAtt;
  private final PayloadAttribute payloadAtt;

  VIntPayloadCodec codec = new VIntPayloadCodec();

  public SirenPayloadFilter(final TokenStream input) {
    super(input);
    payloadAtt = this.addAttribute(PayloadAttribute.class);
    nodeAtt = this.addAttribute(NodeAttribute.class);
    posAtt = this.addAttribute(PositionAttribute.class);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      // encode node path
      final BytesRef bytes = codec.encode(nodeAtt.node(), posAtt.position());
      payloadAtt.setPayload(bytes);
      return true;
    }
    else {
      return false;
    }
  }

}
