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

import org.apache.lucene.util.IntsRef;
import org.junit.Before;
import org.junit.Test;

import com.sindicetech.siren.analysis.ConciseJsonTokenizer;

import java.io.StringReader;

import static com.sindicetech.siren.analysis.ExtendedJsonTokenizer.*;
import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static com.sindicetech.siren.util.XSDDatatype.*;

public class TestConciseJsonTokenizer extends NodeTokenizerTestCase {

  private ConciseJsonTokenizer _t;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    _t = new ConciseJsonTokenizer(new StringReader(""));
  }

  @Test
  public void testSimple() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : \"b\" }",
      new String[] { "b" },
      new String[] { "a" },
      new String[] { TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING },
      new int[] { 1 },
      new IntsRef[] { node(0) });
  }

  @Test
  public void testNumber() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : 3.2 }",
      new String[] { "3.2" },
      new String[] { "a" },
      new String[] { TOKEN_TYPES[NUMBER] },
      new String[] { XSD_DOUBLE },
      new int[] { 1 },
      new IntsRef[] { node(0) });
  }

  @Test
  public void testArray() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : [1, 2, 3] }",
      new String[] { "1", "2", "3" },
      new String[] { "a", "a", "a" },
      new String[] { TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER] },
      new String[] { XSD_LONG, XSD_LONG, XSD_LONG },
      new int[] { 1, 1, 1 },
      new IntsRef[] { node(0), node(1), node(2) });
  }

  @Test
  public void testEmptyArray() throws Exception {
    // should produce no tokens
    this.assertTokenizesTo(_t, "{ \"a\" : [] }",
      new String[0],
      new String[0]);
  }

  @Test
  public void testEmptyObject() throws Exception {
    // should produce an empty token
    this.assertTokenizesTo(_t, "{ \"a\" : {} }",
      new String[] { "" },
      new String[] { "a" },
      new String[] { TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING },
      new int[] { 1 },
      new IntsRef[] { node(0) });
  }

  @Test
  public void testMultipleFields() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a1\" : \"b1\", \"a2\" : \"b2\" }",
      new String[] { "b1", "b2" },
      new String[] { "a1", "a2" },
      new String[] { TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING, XSD_STRING },
      new int[] { 1, 1 },
      new IntsRef[] { node(0), node(1) });
  }

  @Test
  public void testNestedObject() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : { \"b\" : \"c\" } }",
      new String[] { "", "c" },
      new String[] { "a", "b" },
      new String[] { TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING, XSD_STRING },
      new int[] { 1, 1 },
      new IntsRef[] { node(0), node(0,0) });

    this.assertTokenizesTo(_t, "{ \"a\" : { \"b\" : { \"c\" : \"d\" } } }",
      new String[] { "", "", "d" },
      new String[] { "a", "b", "c" },
      new String[] { TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING, XSD_STRING, XSD_STRING },
      new int[] { 1, 1, 1 },
      new IntsRef[] { node(0), node(0,0), node(0,0,0) });

    this.assertTokenizesTo(_t, "{ \"a\" : { \"b1\" : \"c1\", \"b2\" : \"c2\" } }",
      new String[] { "", "c1", "c2" },
      new String[] { "a", "b1", "b2" },
      new String[] { TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING, XSD_STRING, XSD_STRING },
      new int[] { 1, 1, 1 },
      new IntsRef[] { node(0), node(0,0), node(0,1) });
  }

  @Test
  public void testNestedArrayOfObjects() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : [ { \"b1\" : \"c1\" }, { \"b2\" : \"c2\" } ] }",
      new String[] { "", "c1", "", "c2" },
      new String[] { "a", "b1", "a", "b2" },
      new String[] { TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL] },
      new String[] { XSD_STRING, XSD_STRING, XSD_STRING, XSD_STRING },
      new int[] { 1, 1, 1, 1 },
      new IntsRef[] { node(0), node(0,0), node(1), node(1,0) });
  }

  @Test
  public void testMixedArrays() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : [ 1, { \"b\" : \"c\" } ] }",
      new String[] { "1", "", "c" },
      new String[] { "a", "a", "b" },
      new String[] { TOKEN_TYPES[NUMBER], TOKEN_TYPES[LITERAL], TOKEN_TYPES[LITERAL] },
      new String[] { XSD_LONG, XSD_STRING, XSD_STRING },
      new int[] { 1, 1, 1, 1 },
      new IntsRef[] { node(0), node(1), node(1,0) });
  }

  @Test
  public void testNestedArrays() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : [ 1, [2, 3], 4 ] }",
      new String[] { "1", "2", "3", "4" },
      new String[] { "a", "a", "a", "a" },
      new String[] { TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER] },
      new String[] { XSD_LONG, XSD_LONG, XSD_LONG, XSD_LONG },
      new int[] { 1, 1, 1, 1 },
      new IntsRef[] { node(0), node(1,0), node(1,1), node(2) });
  }

  @Test
  public void testFlattenNestedArrays() throws Exception {
    _t.setFlattenNestedArray(true);
    this.assertTokenizesTo(_t, "{ \"a\" : [ 1, [2, 3], 4 ] }",
    new String[] { "1", "2", "3", "4" },
    new String[] { "a", "a", "a", "a" },
    new String[] { TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER], TOKEN_TYPES[NUMBER] },
    new String[] { XSD_LONG, XSD_LONG, XSD_LONG, XSD_LONG },
    new int[] { 1, 1, 1, 1 },
    new IntsRef[] { node(0), node(1), node(2), node(3) });
  }

  @Test
  public void testDatatype() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : { \"_datatype_\" : \"test\", \"_value_\" : \"b\" } }",
      new String[] { "b" },
      new String[] { "a" },
      new String[] { TOKEN_TYPES[LITERAL] },
      new String[] { "test" },
      new int[] { 1 },
      new IntsRef[] { node(0) });
  }

  @Test(expected=IllegalStateException.class)
  public void testIncompleteDatatype() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : { \"_datatype_\" : \"test\" } }");
  }

  @Test(expected=IllegalStateException.class)
  public void testInvalidDatatype() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : { \"_datatype_\" : \"test\", \"b\" : 3 } }");
  }

  @Test(expected=IllegalStateException.class)
  public void testDatatypeKeywordInObject() throws Exception {
    this.assertTokenizesTo(_t, "{ \"a\" : { \"b\" : 3, \"_datatype_\" : \"test\" } }");
  }

}
