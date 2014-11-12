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
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;

import com.sindicetech.siren.analysis.attributes.DatatypeAttribute;
import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.PathAttribute;

import java.io.StringReader;

public abstract class NodeTokenizerTestCase extends LuceneTestCase {

  /**
   * Execute the tokenizer on the given input. Used to validate the exceptions thrown by the tokenizer.
   */
  protected void assertTokenizesTo(final Tokenizer t, final String input) throws Exception {
    t.setReader(new StringReader(input));
    t.reset(); // reset the stream for the new reader

    while (t.incrementToken()) {
      // do nothing
    }

    t.end();
    t.close();
  }

  protected void assertTokenizesTo(final Tokenizer t, final String input,
                                   final String[] expectedImages,
                                   final String[] expectedTypes)
  throws Exception {
    this.assertTokenizesTo(t, input, expectedImages, null, expectedTypes, null, null, null);
  }

  protected void assertTokenizesTo(final Tokenizer t, final String input,
                                   final String[] expectedImages,
                                   final String[] expectedTypes,
                                   final String[] expectedDatatypes)
  throws Exception {
    this.assertTokenizesTo(t, input, expectedImages, null, expectedTypes, expectedDatatypes, null, null);
  }

  protected void assertTokenizesTo(final Tokenizer t, final String input,
                                   final String[] expectedImages,
                                   final String[] expectedTypes,
                                   final int[] expectedPosIncrs,
                                   final IntsRef[] expectedNode)
  throws Exception {
    this.assertTokenizesTo(t, input, expectedImages, null, expectedTypes, null,
      expectedPosIncrs, expectedNode);
  }

  protected void assertTokenizesTo(final Tokenizer t, final String input,
                                   final String[] expectedImages,
                                   final String[] expectedTypes,
                                   final String[] expectedDatatypes,
                                   final int[] expectedPosIncrs,
                                   final IntsRef[] expectedNode)
  throws Exception {
    this.assertTokenizesTo(t, input, expectedImages, null, expectedTypes,
      expectedDatatypes, expectedPosIncrs, expectedNode);
  }

  protected void assertTokenizesTo(final Tokenizer t, final String input,
                                   final String[] expectedImages,
                                   final String[] expectedFields,
                                   final String[] expectedTypes,
                                   final String[] expectedDatatypes,
                                   final int[] expectedPosIncrs,
                                   final IntsRef[] expectedNode)
  throws Exception {

    assertTrue("has TermAttribute", t.hasAttribute(CharTermAttribute.class));
    final CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);

    PathAttribute fieldAtt = null;
    if (expectedFields != null) {
      assertTrue("has FieldAttribute", t.hasAttribute(PathAttribute.class));
      fieldAtt = t.getAttribute(PathAttribute.class);
    }

    TypeAttribute typeAtt = null;
    if (expectedTypes != null) {
      assertTrue("has TypeAttribute", t.hasAttribute(TypeAttribute.class));
      typeAtt = t.getAttribute(TypeAttribute.class);
    }

    DatatypeAttribute dtypeAtt = null;
    if (expectedDatatypes != null) {
      assertTrue("has DatatypeAttribute", t.hasAttribute(DatatypeAttribute.class));
      dtypeAtt = t.getAttribute(DatatypeAttribute.class);
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

    t.setReader(new StringReader(input));
    t.reset(); // reset the stream for the new reader

    for (int i = 0; i < expectedImages.length; i++) {

      assertTrue("token " + i +" exists", t.incrementToken());

      assertEquals("i=" + i, expectedImages[i], termAtt.toString());

      if (expectedFields != null) {
        assertEquals("i=" + i, expectedFields[i], fieldAtt.field());
      }

      if (expectedTypes != null) {
        assertEquals("i=" + i, expectedTypes[i], typeAtt.type());
      }

      if (expectedDatatypes != null) {
        assertEquals("i=" + i, expectedDatatypes[i], dtypeAtt.datatypeURI() == null ? "" : String.valueOf(dtypeAtt.datatypeURI()));
      }

      if (expectedPosIncrs != null) {
        assertEquals("i=" + i, expectedPosIncrs[i], posIncrAtt.getPositionIncrement());
      }

      if (expectedNode != null) {
        assertEquals("i=" + i, expectedNode[i], nodeAtt.node());
      }

    }

    assertFalse("end of stream", t.incrementToken());
    t.end();
    t.close();
  }

  protected void print(final Tokenizer t, final String input) throws Exception {
    final CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);

    TypeAttribute typeAtt =  t.getAttribute(TypeAttribute.class);

    DatatypeAttribute dtypeAtt = t.getAttribute(DatatypeAttribute.class);

    PositionIncrementAttribute posIncrAtt = t.getAttribute(PositionIncrementAttribute.class);

    NodeAttribute nodeAtt = t.getAttribute(NodeAttribute.class);

    PathAttribute fieldAtt = t.getAttribute(PathAttribute.class);

    t.setReader(new StringReader(input));
    t.reset(); // reset the stream for the new reader

    StringBuilder builder = new StringBuilder();
    while (t.incrementToken()) {
      builder.setLength(0);
      builder.append(fieldAtt.field());
      builder.append(", ");
      builder.append(termAtt.toString());
      builder.append(", ");
      builder.append(typeAtt.type());
      builder.append(", ");
      builder.append(dtypeAtt.datatypeURI() == null ? "" : String.valueOf(dtypeAtt.datatypeURI()));
      builder.append(", ");
      builder.append(posIncrAtt.getPositionIncrement());
      builder.append(", ");
      builder.append(nodeAtt.node());
      System.out.println(builder.toString());
    }

    t.end();
    t.close();
  }

}
