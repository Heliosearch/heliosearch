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
import org.apache.lucene.search.Query;
import org.junit.Test;

import com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser;
import com.sindicetech.siren.qparser.tree.dsl.*;

import static org.junit.Assert.assertEquals;

public class BooleanSpanQueryTest {

  @Test
  public void testBooleanSpanWithNode() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newBoolean()
                                           .with(build.newNode("aaa"))
                                           .with(build.newNode("bbb"))
                                           .setSlop(1));
    assertParser(twig);
  }

  @Test
  public void testBooleanSpanWithTwig() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newBoolean()
                                           .with(build.newTwig("aa")
                                                      .with(build.newNode("aaa")))
                                           .with(build.newTwig("bb")
                                                      .with(build.newNode("bbb")))
                                           .setInOrder(false));
    assertParser(twig);
  }

  @Test
  public void testNestedBooleanSpan() throws QueryNodeException {
    final QueryBuilder build = new QueryBuilder();
    final TwigQuery twig = build.newTwig()
                                .with(build.newBoolean()
                                           .with(build.newBoolean()
                                                      .optional(build.newNode("aaa"))
                                                      .optional(build.newNode("ccc"))
                                                      .setInOrder(true))
                                           .with(build.newNode("bbb"))
                                           .setSlop(1));
    assertParser(twig);
  }

  private static void assertParser(final AbstractQuery query) throws QueryNodeException {
    final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
    final Query output = parser.parse(query.toString(), "");
    assertEquals(query.toQuery(true), output);
  }

}
