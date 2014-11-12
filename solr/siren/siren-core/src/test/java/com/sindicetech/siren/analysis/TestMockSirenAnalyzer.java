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

import static com.sindicetech.siren.analysis.MockSirenDocument.doc;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.analysis.MockSirenToken.token;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;

import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.PositionAttribute;

public class TestMockSirenAnalyzer {

  @Test
  public void testMockSirenDocument() {
    final MockSirenDocument doc = doc(token("aaa", node(1)), token("aaa", node(1,0)), token("aaa", node(1)));
    final Iterator<ArrayList<MockSirenToken>> nodeIt = doc.iterator();

    assertTrue(nodeIt.hasNext());
    ArrayList<MockSirenToken> tokens = nodeIt.next();
    assertEquals(2, tokens.size());
    assertEquals(node(1), tokens.get(0).nodePath);
    assertEquals(node(1), tokens.get(1).nodePath);

    assertTrue(nodeIt.hasNext());
    tokens = nodeIt.next();
    assertEquals(1, tokens.size());
    assertEquals(node(1,0), tokens.get(0).nodePath);
  }

  @Test
  public void testMockSirenAnalyzer() throws IOException {
    final MockSirenDocument doc = doc(token("aaa", node(1)), token("aaa", node(1,0)), token("aaa", node(1)));
    final MockSirenAnalyzer analyzer = new MockSirenAnalyzer();
    final TokenStream ts = analyzer.tokenStream("", new MockSirenReader(doc));

    assertTrue(ts.incrementToken());
    assertEquals("aaa", ts.getAttribute(CharTermAttribute.class).toString());
    assertEquals(node(1), ts.getAttribute(NodeAttribute.class).node());
    assertEquals(0, ts.getAttribute(PositionAttribute.class).position());

    assertTrue(ts.incrementToken());
    assertEquals("aaa", ts.getAttribute(CharTermAttribute.class).toString());
    assertEquals(node(1), ts.getAttribute(NodeAttribute.class).node());
    assertEquals(1, ts.getAttribute(PositionAttribute.class).position());

    assertTrue(ts.incrementToken());
    assertEquals("aaa", ts.getAttribute(CharTermAttribute.class).toString());
    assertEquals(node(1,0), ts.getAttribute(NodeAttribute.class).node());
    assertEquals(0, ts.getAttribute(PositionAttribute.class).position());

  }

}
