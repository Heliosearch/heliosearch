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
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Test;

import com.sindicetech.siren.analysis.ExtendedJsonAnalyzer;
import com.sindicetech.siren.util.XSDDatatype;

public class TestExtendedJsonAnalyzer
extends NodeAnalyzerTestCase<ExtendedJsonAnalyzer> {

  @Override
  protected ExtendedJsonAnalyzer getNodeAnalyzer() {
    final Analyzer literalAnalyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);
    final Analyzer fieldAnalyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    return new ExtendedJsonAnalyzer(fieldAnalyzer, literalAnalyzer);
  }

  @Test
  public void testLiteral()
  throws Exception {
    this.assertAnalyzesTo(_a, "{\"foo BAR\":[null,\"FOO bar\"]}", // null is typed as XSD_STRING
      new String[] { "foo", "BAR", "null", "foo", "bar" },
      new String[] { TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE,
                     "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>" });
    this.assertAnalyzesTo(_a, "{\"ABC\\u0061\\u0062\\u0063\\u00E9\\u00e9ABC\":\"EmptY\"}",
      new String[] { "ABCabcééABC", "empty" },
      new String[] { TypeAttribute.DEFAULT_TYPE, "<ALPHANUM>" });
  }

  @Test
  public void testLong()
  throws Exception {
    _a.registerDatatype(XSDDatatype.XSD_LONG.toCharArray(), new StandardAnalyzer(TEST_VERSION_CURRENT));
    this.assertAnalyzesTo(_a, "{\"foo\":12}",
      new String[] { "foo", "12" },
      new String[] { TypeAttribute.DEFAULT_TYPE, "<NUM>" });
  }

  @Test
  public void testDouble()
  throws Exception {
    _a.registerDatatype(XSDDatatype.XSD_DOUBLE.toCharArray(), new StandardAnalyzer(TEST_VERSION_CURRENT));
    this.assertAnalyzesTo(_a, "{\"foo\":12.42}",
      new String[] { "foo", "12.42" },
      new String[] { TypeAttribute.DEFAULT_TYPE, "<NUM>" });
  }

  @Test
  public void testBoolean()
  throws Exception {
    _a.registerDatatype(XSDDatatype.XSD_BOOLEAN.toCharArray(), new WhitespaceAnalyzer(TEST_VERSION_CURRENT));
    this.assertAnalyzesTo(_a, "{\"foo\":[true,false]}",
      new String[] { "foo", "true", "false" },
      new String[] { TypeAttribute.DEFAULT_TYPE, TypeAttribute.DEFAULT_TYPE,
                     TypeAttribute.DEFAULT_TYPE });
  }

}
