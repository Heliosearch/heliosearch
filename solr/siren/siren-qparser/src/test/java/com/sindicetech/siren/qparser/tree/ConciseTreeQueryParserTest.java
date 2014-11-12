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

import com.sindicetech.siren.qparser.tree.dsl.AbstractQuery;
import com.sindicetech.siren.qparser.tree.dsl.ConciseQueryBuilder;
import com.sindicetech.siren.qparser.tree.dsl.NodeQuery;
import com.sindicetech.siren.qparser.tree.dsl.TwigQuery;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConciseTreeQueryParserTest {

  @Test
  public void testNoAttribute() throws QueryNodeException {
    final ConciseQueryBuilder build = new ConciseQueryBuilder();
    final NodeQuery node = build.newNode("aaa");
    assertParser(node);
  }

  @Test
  public void testNodeWithQuery() throws QueryNodeException {
    final ConciseQueryBuilder build = new ConciseQueryBuilder();
    final NodeQuery node = build.newNode("bbb").setAttribute("aaa");
    assertParser(node);
  }

  @Test
  public void testTwigWithRoot() throws QueryNodeException {
    final ConciseQueryBuilder build = new ConciseQueryBuilder();
    final TwigQuery twig = build.newTwig("aaa");
    assertParser(twig);
  }

  /**
   * See #15
   */
  @Test
  public void testTwigWithSetAndUnsetAttribute() throws QueryNodeException {
    final ConciseQueryBuilder build = new ConciseQueryBuilder();
    final TwigQuery twig = build.newTwig("funding")
                                .with(build.newNode("a").setAttribute("round"))
                                .with(build.newNode("2006"));
    assertParser(twig);
  }

  /**
   * Checks that a keyword query that returns an {@link org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode}
   * is converted into an empty {@link com.sindicetech.siren.search.node.NodeBooleanQuery}.
   *
   * See #73.
   */
  @Test
  public void testKeywordParserReturnsMatchNoDoc() throws QueryNodeException {
    final ConciseTreeQueryParser parser = new ConciseTreeQueryParser();
    final Query output = parser.parse("{\"node\":{\"query\":\"\"}}", "");
    assertTrue(output instanceof LuceneProxyNodeQuery);
    com.sindicetech.siren.search.node.NodeQuery nodeQuery = ((LuceneProxyNodeQuery) output).getNodeQuery();
    assertTrue(nodeQuery instanceof NodeBooleanQuery);
    assertTrue(((NodeBooleanQuery) nodeQuery).clauses().isEmpty());
  }

  private static void assertParser(final AbstractQuery query) throws QueryNodeException {
    final ConciseTreeQueryParser parser = new ConciseTreeQueryParser();
    final Query output = parser.parse(query.toString(), "");
    assertEquals(query.toQuery(true), output);
  }

}
