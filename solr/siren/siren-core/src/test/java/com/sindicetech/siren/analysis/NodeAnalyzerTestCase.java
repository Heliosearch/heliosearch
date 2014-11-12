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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.PositionAttribute;

import java.io.StringReader;

public abstract class NodeAnalyzerTestCase<A extends Analyzer> extends LuceneTestCase {

  protected A _a;

  protected abstract A getNodeAnalyzer();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    _a = this.getNodeAnalyzer();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    _a.close();
  }

  public void assertAnalyzesTo(final Analyzer a, final String input,
                                final String[] expected)
  throws Exception {
    this.assertAnalyzesTo(a, input, expected, null);
  }

  public void assertAnalyzesTo(final Analyzer a, final String input,
                                final String[] expectedImages,
                                final String[] expectedTypes)
  throws Exception {
    this.assertAnalyzesTo(a, input, expectedImages, expectedTypes, null);
  }

  public void assertAnalyzesTo(final Analyzer a, final String input,
                                final String[] expectedImages,
                                final String[] expectedTypes,
                                final int[] expectedPosIncrs)
  throws Exception {
    this.assertAnalyzesTo(a, input, expectedImages, expectedTypes, expectedPosIncrs, null,
      null);
  }

  public void assertAnalyzesTo(final Analyzer a, final String input,
                                final String[] expectedImages,
                                final String[] expectedTypes,
                                final int[] expectedPosIncrs,
                                final IntsRef[] expectedNode,
                                final int[] expectedPos)
  throws Exception {
    final TokenStream t = a.tokenStream("", new StringReader(input));
    t.reset();

    assertTrue("has TermAttribute", t.hasAttribute(CharTermAttribute.class));
    final CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);

    TypeAttribute typeAtt = null;
    if (expectedTypes != null) {
      assertTrue("has TypeAttribute", t.hasAttribute(TypeAttribute.class));
      typeAtt = t.getAttribute(TypeAttribute.class);
    }

    PositionIncrementAttribute posIncrAtt = null;
    if (expectedPosIncrs != null) {
      assertTrue("has PositionIncrementAttribute", t.hasAttribute(PositionIncrementAttribute.class));
      posIncrAtt = t.getAttribute(PositionIncrementAttribute.class);
    }

    NodeAttribute nodeAtt = null;
    if (expectedNode != null) {
      assertTrue("has NodeAttribute", t.hasAttribute(NodeAttribute.class));
      nodeAtt = t.getAttribute(NodeAttribute.class);
    }

    PositionAttribute posAtt = null;
    if (expectedPos != null) {
      assertTrue("has PositionAttribute", t.hasAttribute(PositionAttribute.class));
      posAtt = t.getAttribute(PositionAttribute.class);
    }

    for (int i = 0; i < expectedImages.length; i++) {

      assertTrue("token "+i+" exists", t.incrementToken());

      assertEquals("i=" + i, expectedImages[i], termAtt.toString());

      if (expectedTypes != null) {
        assertEquals(expectedTypes[i], typeAtt.type());
      }

      if (expectedPosIncrs != null) {
        assertEquals(expectedPosIncrs[i], posIncrAtt.getPositionIncrement());
      }

      if (expectedNode != null) {
        assertEquals(expectedNode[i], nodeAtt.node());
      }

      if (expectedPos != null) {
        assertEquals(expectedPos[i], posAtt.position());
      }
    }

    assertFalse("end of stream, received token " + termAtt.toString(), t.incrementToken());
    t.end();
    t.close();
  }

}
