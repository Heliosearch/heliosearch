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

package com.sindicetech.siren.search.spans;

import org.junit.Test;

import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;
import com.sindicetech.siren.util.SirenTestCase;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestNodeSpanQuery extends SirenTestCase {

  @Test
  public void testEquality() throws Exception {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("aaa").getQuery());

    assertEquals(term1, term2);

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    term3.setLevelConstraint(3);
    assertNotEquals(term1, term3);

    NodeSpanQuery term4 = new NodeSpanQuery(ntq("aaa").getQuery());
    term4.setNodeConstraint(5);
    assertNotEquals(term1, term4);
  }

  @Test
  public void testSetLevelConstraint() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());

    term1.setLevelConstraint(3);
    assertEquals(3, term1.getLevelConstraint());
    // Level constraint must have been transferred to the inner query
    assertEquals(3, term1.getQuery().getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());

    final TwigQuery twig = new TwigQuery();

    term1.setAncestorPointer(twig);

    assertSame(twig, term1.getAncestorPointer());
    // inner query must have been updated
    assertSame(twig, term1.getQuery().getAncestorPointer());
  }

}
