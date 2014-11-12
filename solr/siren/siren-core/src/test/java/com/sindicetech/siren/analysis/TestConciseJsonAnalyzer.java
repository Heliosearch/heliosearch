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
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Test;

import com.sindicetech.siren.analysis.ConciseJsonAnalyzer;
import com.sindicetech.siren.analysis.ExtendedJsonAnalyzer;
import com.sindicetech.siren.analysis.LongNumericAnalyzer;
import com.sindicetech.siren.util.XSDDatatype;

import java.io.StringReader;

public class TestConciseJsonAnalyzer extends NodeAnalyzerTestCase<ExtendedJsonAnalyzer> {

  @Override
  protected ExtendedJsonAnalyzer getNodeAnalyzer() {
    final Analyzer literalAnalyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    final Analyzer fieldAnalyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    return new ConciseJsonAnalyzer(fieldAnalyzer, literalAnalyzer);
  }

  @Test
  public void testPathEncoding() throws Exception {
    this.assertAnalyzesTo(_a, "{ \"a\" : [null, \"b\" ] }",
      new String[] { "a:null", "a:b" },
      new String[] { TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE });

    this.assertAnalyzesTo(_a, "{ \"a\" : { \"b\" : \"c\" } }",
      new String[] { "a:", "b:c" },
      new String[] { "<LITERAL>", TypeAttribute.DEFAULT_TYPE });
  }

  @Test
  public void testPathEncodingAttributeWildcardOriginal() throws Exception {
    final Analyzer literalAnalyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    final Analyzer fieldAnalyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    ConciseJsonAnalyzer analyzer = new ConciseJsonAnalyzer(fieldAnalyzer, literalAnalyzer);
    analyzer.setGenerateTokensWithoutPath(true);

    this.assertAnalyzesTo(analyzer, "{ \"a\" : [null, \"b\" ] }",
      new String[] { "null", "a:null", "b", "a:b" },
      new String[] { TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE },
      new int[] { 1, 0, 1, 0 });

    this.assertAnalyzesTo(analyzer, "{ \"a\" : { \"b\" : \"c\" } }",
      new String[] { "a:", "c", "b:c" },
      new String[] { "<LITERAL>", TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE },
      new int[] { 1, 1, 0 });
  }

  @Test
  public void testNumeric() throws Exception {
    _a.registerDatatype(XSDDatatype.XSD_LONG.toCharArray(), new LongNumericAnalyzer(64));
    final TokenStream t = _a.tokenStream("", new StringReader("{ \"a\" : 12 }"));
    final CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    t.reset();
    assertTrue(t.incrementToken());
    assertTrue(termAtt.toString().startsWith("a:"));
    t.end();
    t.close();
  }

}
