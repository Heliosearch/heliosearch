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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;

import com.sindicetech.siren.analysis.filter.ASCIIFoldingExpansionFilter;

public class TestASCIIFoldingExpansionFilter extends LuceneTestCase {

  @Test
  public void testTokenTypeFilter1() throws Exception {
    final Reader reader = new StringReader("aaa clés café");
    final TokenStream stream = new WhitespaceTokenizer(TEST_VERSION_CURRENT, reader);
    final ASCIIFoldingExpansionFilter filter = new ASCIIFoldingExpansionFilter(stream);

    final CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    final PositionIncrementAttribute posAtt = filter.getAttribute(PositionIncrementAttribute.class);

    filter.reset(); // prepare stream

    this.assertTermEquals("aaa", 1, filter, termAtt, posAtt);
    this.assertTermEquals("cles", 1, filter, termAtt, posAtt);
    this.assertTermEquals("clés", 0, filter, termAtt, posAtt);
    this.assertTermEquals("cafe", 1, filter, termAtt, posAtt);
    this.assertTermEquals("café", 0, filter, termAtt, posAtt);
  }

  void assertTermEquals(final String termExpected, final int posIncExpected, final TokenStream stream,
                        final CharTermAttribute termAtt, final PositionIncrementAttribute posAtt)
  throws Exception {
    Assert.assertTrue(stream.incrementToken());
    Assert.assertEquals(termExpected, termAtt.toString());
    Assert.assertEquals(posIncExpected, posAtt.getPositionIncrement());
  }



}
