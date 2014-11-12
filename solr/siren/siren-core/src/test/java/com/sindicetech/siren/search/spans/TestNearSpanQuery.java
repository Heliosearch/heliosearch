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
import com.sindicetech.siren.search.spans.NearSpanQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;
import com.sindicetech.siren.util.SirenTestCase;

import static com.sindicetech.siren.search.AbstractTestSirenScorer.NodeTermQueryBuilder.ntq;

public class TestNearSpanQuery extends SirenTestCase {

  @Test
  public void testEquality() throws Exception {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    final NearSpanQuery nsq1 = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("bbb").getQuery());
    final NearSpanQuery nsq2 = new NearSpanQuery(new NodeSpanQuery[] {term3, term4}, 0, true);

    assertEquals(nsq1, nsq2);

    final NearSpanQuery nsq3 = new NearSpanQuery(new NodeSpanQuery[] {term3, term4}, 1, true);

    assertNotEquals(nsq1, nsq3);

    NodeSpanQuery term5 = new NodeSpanQuery(ntq("ccc").getQuery());
    final NearSpanQuery nsq4 = new NearSpanQuery(new NodeSpanQuery[] {term1, term5}, 0, true);

    assertNotEquals(nsq1, nsq4);

    final NearSpanQuery nsq5 = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    nsq5.setLevelConstraint(3);
    assertNotEquals(nsq1, nsq5);

    final NearSpanQuery nsq6 = new NearSpanQuery(new NodeSpanQuery[] {term1, term2}, 0, true);
    nsq6.setNodeConstraint(5);
    assertNotEquals(nsq1, nsq6);
  }

  @Test
  public void testSetLevelConstraint() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    final NearSpanQuery nsq1 = new NearSpanQuery(new SpanQuery[] {term1, term2}, 0, true);

    nsq1.setLevelConstraint(3);
    assertEquals(3, nsq1.getLevelConstraint());
    // Level constraint must have been transferred to the clauses
    assertEquals(3, nsq1.getClauses()[0].getLevelConstraint());
    assertEquals(3, nsq1.getClauses()[1].getLevelConstraint());

    NodeSpanQuery term3 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term4 = new NodeSpanQuery(ntq("bbb").getQuery());
    final NearSpanQuery nsq2 = new NearSpanQuery(new SpanQuery[] {term3, term4}, 0, true);
    nsq2.setLevelConstraint(4);

    final NearSpanQuery nsq3 = new NearSpanQuery(new SpanQuery[] {nsq1, nsq2}, 0, true);
    nsq3.setLevelConstraint(6);

    // Level constraint must have been transferred to the clauses
    assertEquals(6, nsq1.getLevelConstraint());
    assertEquals(6, nsq2.getLevelConstraint());
  }

  @Test
  public void testSetAncestorPointer() {
    NodeSpanQuery term1 = new NodeSpanQuery(ntq("aaa").getQuery());
    NodeSpanQuery term2 = new NodeSpanQuery(ntq("bbb").getQuery());
    final NearSpanQuery nsq1 = new NearSpanQuery(new SpanQuery[] {term1, term2}, 0, true);

    final TwigQuery twig = new TwigQuery();

    nsq1.setAncestorPointer(twig);

    assertSame(twig, nsq1.getAncestorPointer());
    // clauses must have been updated
    assertSame(twig, term1.getAncestorPointer());
  }

}
