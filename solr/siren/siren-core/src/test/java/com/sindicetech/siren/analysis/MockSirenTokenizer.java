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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.analysis.TupleTokenizer;
import com.sindicetech.siren.analysis.attributes.DatatypeAttribute;
import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.TupleNodeAttributeImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class MockSirenTokenizer extends Tokenizer {

  MockSirenDocument doc;

  // the TupleTokenizer generates 6 attributes:
  // term, offset, positionIncrement, type, datatype, node
  private final CharTermAttribute termAtt;
  private final OffsetAttribute offsetAtt;
  private final PositionIncrementAttribute posIncrAtt;
  private final TypeAttribute typeAtt;
  private final DatatypeAttribute dtypeAtt;
  private final NodeAttribute nodeAtt;

  Iterator<ArrayList<MockSirenToken>> nodeIt = null;
  Iterator<MockSirenToken> tokenIt = null;

  protected static final Logger logger = LoggerFactory.getLogger(MockSirenTokenizer.class);

  public MockSirenTokenizer(final MockSirenReader reader) {
    super(reader);

    this.doc = reader.getDocument();
    nodeIt = doc.iterator();

    termAtt = this.addAttribute(CharTermAttribute.class);
    offsetAtt = this.addAttribute(OffsetAttribute.class);
    posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);
    typeAtt = this.addAttribute(TypeAttribute.class);
    dtypeAtt = this.addAttribute(DatatypeAttribute.class);
    if (!this.hasAttribute(NodeAttribute.class)) {
      this.addAttributeImpl(new TupleNodeAttributeImpl());
    }
    nodeAtt = this.addAttribute(NodeAttribute.class);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    this.clearAttributes();

    final MockSirenToken token;
    while (nodeIt.hasNext() || (tokenIt != null && tokenIt.hasNext())) {
      if (tokenIt == null || !tokenIt.hasNext()) { // new node
        tokenIt = nodeIt.next().iterator(); // move to next node
      }

      token = tokenIt.next();
      termAtt.copyBuffer(token.term, 0, token.term.length);
      offsetAtt.setOffset(token.startOffset, token.endOffset);
      typeAtt.setType(TupleTokenizer.getTokenTypes()[token.tokenType]);
      posIncrAtt.setPositionIncrement(token.posInc);
      dtypeAtt.setDatatypeURI(token.datatype);
      for (int i = 0; i < token.nodePath.length; i++) {
        nodeAtt.append(token.nodePath.ints[i]);
      }
      return true;
    }

    return false;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    final MockSirenReader reader = (MockSirenReader) this.input;
    this.doc = reader.getDocument();
    nodeIt = doc.iterator();
    this.clearAttributes();
  }

}
