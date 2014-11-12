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
package com.sindicetech.siren.qparser.tree;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.junit.Test;

import com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser;
import com.sindicetech.siren.qparser.tree.ParseException;
import com.sindicetech.siren.qparser.tree.dsl.*;
import com.sindicetech.siren.search.node.DatatypedNodeQuery;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeVariableQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause.Occur;
import com.sindicetech.siren.util.XSDDatatype;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class ExtendedTreeQueryParserTest {

  @Test(expected=ParseException.class)
  public void testEmptyQuery() throws QueryNodeException {
    final String query = "{ }";
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testUnknownTopLevelField() throws QueryNodeException {
    final String query = "{ \"unknown\" : { } }";
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    parser.parse(query, "");
  }

  @Test
  public void testMoreThanOneTopLevelQuery() throws QueryNodeException {
    final String query = "{ \"node\" : { \"query\" : \"aaa\" }, \"twig\" : { \"query\" : \"bbb\" } }";
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    // must not failed as it will take only the first top-level field encountered
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testNodeWithInvalidQuery() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"node\" : { \"query\" : 132 } }";
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testNodeWithInvalidLevel() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"node\" : { \"query\" : \"aaa\", \"level\" : \"3\" } }";
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testNodeWithInvalidRange1() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"node\" : { \"query\" : \"aaa\", \"range\" : 3 } }";
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testNodeWithInvalidRange2() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"node\" : { \"query\" : \"aaa\", \"range\" : [3] } }";
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testNodeWithInvalidRange3() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"node\" : { \"query\" : \"aaa\", \"range\" : [2.34, 3.45] } }";
    parser.parse(query, "");
  }

  @Test(expected=ParseException.class)
  public void testNodeWithNoQuery() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"node\" : { \"level\" : 1 } }";
    parser.parse(query, "");
  }

  @Test
  public void testNodeWithQuery() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final NodeQuery node = build.newNode("aaa");
    assertParser(node);
  }

  @Test
  public void testNodeWithLevelAndRange() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final NodeQuery node = build.newNode("aaa")
                           .setLevel(1)
                           .setRange(1, 2);
    assertParser(node);
  }

  @Test(expected=ParseException.class)
  public void testTwigWithInvalidChild() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"twig\" : { \"child\" : " +
    		"{ \"occur\" : \"MUST\", \"node\" : { \"query\" : \"aaa\" } } " +
    		"} }";
    parser.parse(query, "");
  }

  @Test
  public void testTwigWithChildWithMissingOccur() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"twig\" : { \"child\" : " +
        "[ { \"node\" : { \"query\" : \"aaa\" } } ] " +
        "} }";
    Query q = parser.parse(query, ""); // should not throw exception
    assertTrue(q instanceof LuceneProxyNodeQuery);
    q = ((LuceneProxyNodeQuery) q).getNodeQuery();
    assertTrue(q instanceof com.sindicetech.siren.search.node.TwigQuery);
    Occur occur = ((com.sindicetech.siren.search.node.TwigQuery) q).getClauses()[0].getOccur();
    assertEquals(parser.getDefaultOperator() == StandardQueryConfigHandler.Operator.AND ? Occur.MUST : Occur.SHOULD, occur);
  }

  @Test
  public void testEmptyTwig() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig();
    assertParser(twig);
  }

  @Test
  public void testTwigWithRootOnly() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig("aaa");
    assertParser(twig);
  }

  @Test
  public void testTwigWithRootLevelAndRange() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig("aaa")
                                .setLevel(2)
                                .setRange(2, 5);
    assertParser(twig);
  }

  @Test
  public void testTwigWithVariableAsChild() throws QueryNodeException {
    String query = "{"
          + "\"twig\" : {"
            + "\"child\" : [ {"
              + "\"occur\" : \"MUST\","
              + "\"variable\" : {}"
            + "} ]"
          + "}"
         +"}";
    Query q = new ExtendedTreeQueryParser().parse(query, "");
    com.sindicetech.siren.search.node.NodeQuery nq = ((LuceneProxyNodeQuery)q).getNodeQuery();
    assertThat(nq, instanceOf(com.sindicetech.siren.search.node.TwigQuery.class));
    assertThat(((com.sindicetech.siren.search.node.TwigQuery)nq).getClauses()[0] , instanceOf(NodeBooleanClause.class));
    assertThat(((com.sindicetech.siren.search.node.TwigQuery)nq).getClauses()[0].getQuery() , instanceOf(NodeVariableQuery.class));
    // Occur has to be rewritten to SHOULD in ChildPropertyParser - see the comment there for explanation
    assertEquals(((com.sindicetech.siren.search.node.TwigQuery)nq).getClauses()[0].getOccur() , Occur.SHOULD);
  }

  @Test
  public void testTwigWithVariableAsChildWithoutMust() throws QueryNodeException {
    String query = "{"
          + "\"twig\" : {"
            + "\"child\" : [ {"
              + "\"variable\" : {}"
            + "} ]"
          + "}"
         +"}";
    Query q = new ExtendedTreeQueryParser().parse(query, "");
    com.sindicetech.siren.search.node.NodeQuery nq = ((LuceneProxyNodeQuery)q).getNodeQuery();
    assertThat(nq, instanceOf(com.sindicetech.siren.search.node.TwigQuery.class));
    assertThat(((com.sindicetech.siren.search.node.TwigQuery)nq).getClauses()[0], instanceOf(NodeBooleanClause.class));
    assertThat(((com.sindicetech.siren.search.node.TwigQuery)nq).getClauses()[0].getQuery(), instanceOf(NodeVariableQuery.class));
    // Occur has to be SHOULD by default. See the comment in ChildPropertyParser for explanation
    assertEquals(((com.sindicetech.siren.search.node.TwigQuery)nq).getClauses()[0].getOccur() , Occur.SHOULD);
  }

  @Test
  public void testTwigWithOneChild() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newNode("aaa"));
    assertParser(twig);
  }

  /**
   * Check if the json:field datatype is automatically assigned to the root of the twig
   */
  @Test
  public void testTwigRootDatatype() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    TwigQuery twig = build.newTwig("aaa");

    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    Query output = parser.parse(twig.toString(), "");
    LuceneProxyNodeQuery q = (LuceneProxyNodeQuery) output;
    com.sindicetech.siren.search.node.TwigQuery t = (com.sindicetech.siren.search.node.TwigQuery) q.getNodeQuery();
    com.sindicetech.siren.search.node.NodeQuery root = t.getRoot();

    assertTrue(root instanceof DatatypedNodeQuery);
    String datatype = ((DatatypedNodeQuery) root).getDatatype();
    assertEquals("http://json.org/field", datatype);

    twig = build.newTwig("aaa")
                .optional(
                  build.newTwig("ccc")
                       .with(build.newNode("ddd"))
                );

    output = parser.parse(twig.toString(), "");
    q = (LuceneProxyNodeQuery) output;
    t = (com.sindicetech.siren.search.node.TwigQuery) q.getNodeQuery();
    root = t.getRoot();

    assertTrue(root instanceof DatatypedNodeQuery);
    datatype = ((DatatypedNodeQuery) root).getDatatype();
    assertEquals("http://json.org/field", datatype);

    // check nested twig root
    com.sindicetech.siren.search.node.NodeQuery clause = t.getClauses()[0].getQuery();
    assertTrue(clause instanceof com.sindicetech.siren.search.node.TwigQuery);
    root = ((com.sindicetech.siren.search.node.TwigQuery) clause).getRoot();

    assertTrue(root instanceof DatatypedNodeQuery);
    datatype = ((DatatypedNodeQuery) root).getDatatype();
    assertEquals("http://json.org/field", datatype);

    // check nested node
    com.sindicetech.siren.search.node.NodeQuery node = ((com.sindicetech.siren.search.node.TwigQuery) clause).getClauses()[0].getQuery();
    assertTrue(node instanceof DatatypedNodeQuery);
    datatype = ((DatatypedNodeQuery) node).getDatatype();
    assertEquals(XSDDatatype.XSD_STRING, datatype);
  }

  @Test
  public void testTwigWithMultipleChildren() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newNode("aaa"))
                                .without(build.newNode("bbb"))
                                .optional(build.newTwig()
                                               .with(build.newNode("ccc")));
    assertParser(twig);
  }

  @Test
  public void testTwigWithOneDescendant() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newNode("aaa"), 2);
    assertParser(twig);
  }

  @Test
  public void testTwigWithChildAndDescendant() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newNode("aaa"))
                                .without(build.newNode("bbb"), 2)
                                .optional(build.newTwig()
                                               .with(build.newNode("ccc")), 4);
    assertParser(twig);
  }

  @Test(expected=ParseException.class)
  public void testTopLevelBooleanWithInvalidNonArrayClause() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"boolean\" : " +
    		"{ \"occur\" : \"MUST\", \"node\" : { \"query\" : \"aaa\" } } " +
        "}";
    parser.parse(query, "");
  }

  @Test
  public void testTopLevelBooleanWithMissingOccurInClause() throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final String query = "{ \"boolean\" : { \"clause\" : [ " +
        "{ \"node\" : { \"query\" : \"aaa\" } } " +
        "] } }";
    Query q = parser.parse(query, ""); // should not throw exception
    assertTrue(q instanceof org.apache.lucene.search.BooleanQuery);
    BooleanClause.Occur occur = ((org.apache.lucene.search.BooleanQuery) q).getClauses()[0].getOccur();
    assertEquals(parser.getDefaultOperator() == StandardQueryConfigHandler.Operator.AND ?
      BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD, occur);
  }

  @Test
  public void testBooleanWithOneClause() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final AbstractBooleanQuery bool = build.newBoolean()
                                           .with(build.newNode("aaa"));
    assertParser(bool);
  }

  @Test
  public void testBooleanWithMultipleClauses() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final AbstractBooleanQuery bool = build.newBoolean()
                                           .with(build.newNode("aaa"))
                                           .without(build.newNode("bbb"))
                                           .optional(build.newTwig("ccc").with(build.newNode("ddd")));
    assertParser(bool);
  }

  @Test
  public void testNestedBoolean() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final AbstractBooleanQuery bool = build.newBoolean()
                                           .with(build.newNode("aaa"))
                                           .without(build.newBoolean()
                                                         .with(build.newNode("ddd")));
    assertParser(bool);
  }

  private static void assertParser(final AbstractQuery query) throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final Query output = parser.parse(query.toString(), "");
    assertEquals(query.toQuery(true), output);
  }

}
