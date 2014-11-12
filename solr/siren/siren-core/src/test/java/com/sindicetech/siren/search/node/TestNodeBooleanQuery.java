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
package com.sindicetech.siren.search.node;

import org.apache.lucene.index.Term;
import org.junit.Test;

import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause.Occur;
import com.sindicetech.siren.util.SirenTestCase;

public class TestNodeBooleanQuery extends SirenTestCase {

  @Test
  public void testEquality() throws Exception {
    final NodeBooleanQuery bq1 = new NodeBooleanQuery();
    bq1.add(new NodeTermQuery(new Term("field", "value1")), NodeBooleanClause.Occur.SHOULD);
    bq1.add(new NodeTermQuery(new Term("field", "value2")), NodeBooleanClause.Occur.SHOULD);

    final NodeBooleanQuery bq2 = new NodeBooleanQuery();
    bq2.add(new NodeTermQuery(new Term("field", "value1")), NodeBooleanClause.Occur.SHOULD);
    bq2.add(new NodeTermQuery(new Term("field", "value2")), NodeBooleanClause.Occur.SHOULD);

    assertEquals(bq2, bq1);
  }

  @Test
  public void testSetLevelConstraint() {
    final NodeTermQuery ntq = new NodeTermQuery(new Term("field", "value"));
    final NodeBooleanQuery bq = new NodeBooleanQuery();
    bq.add(ntq, Occur.MUST);
    bq.setLevelConstraint(3);

    assertEquals(3, bq.getLevelConstraint());
    // node queries in node boolean clauses must have been updated
    assertEquals(3, ntq.getLevelConstraint());

    final NodeTermQuery ntq2 = new NodeTermQuery(new Term("field", "value"));
    bq.add(ntq2, Occur.MUST);
    // new clause must have been updated
    assertEquals(3, ntq2.getLevelConstraint());
  }

  @Test
  public void testSetNodeConstraint() {
    final NodeTermQuery ntq = new NodeTermQuery(new Term("field", "value"));
    final NodeBooleanQuery bq = new NodeBooleanQuery();
    bq.add(ntq, Occur.MUST);
    bq.setNodeConstraint(2,6);

    assertEquals(2, bq.lowerBound);
    assertEquals(6, bq.upperBound);
    // node queries in node boolean clauses must have been updated
    assertEquals(2, ntq.lowerBound);
    assertEquals(6, ntq.upperBound);

    final NodeTermQuery ntq2 = new NodeTermQuery(new Term("field", "value"));
    bq.add(ntq2, Occur.MUST);
    // new clause must have been updated
    assertEquals(2, ntq2.lowerBound);
    assertEquals(6, ntq2.upperBound);
  }

  @Test
  public void testSetAncestor() {
    final NodeTermQuery ntq = new NodeTermQuery(new Term("field", "value"));
    final NodeBooleanQuery bq1 = new NodeBooleanQuery();
    bq1.add(ntq, Occur.MUST);

    final NodeBooleanQuery bq2 = new NodeBooleanQuery();

    bq1.setAncestorPointer(bq2);

    assertSame(bq2, bq1.ancestor);
    // node queries in node boolean clauses must have been updated
    assertSame(bq2, ntq.ancestor);

    final NodeTermQuery ntq2 = new NodeTermQuery(new Term("field", "value"));
    bq1.add(ntq2, Occur.MUST);
    // new clause must have been updated
    assertSame(bq2, ntq2.ancestor);
  }

}
