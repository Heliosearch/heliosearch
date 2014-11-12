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
package com.sindicetech.siren.util;

import static com.sindicetech.siren.analysis.MockSirenToken.node;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.IntsRef;
import org.junit.Test;

import com.sindicetech.siren.util.NodeUtils;

public class TestNodeUtils {

  @Test
  public void testCompare() {
    IntsRef n1 = node(0, 0, 0);
    IntsRef n2 = node(1, 0, 0);
    assertTrue(NodeUtils.compare(n1, n2) < 0);

    n1 = node(1, 1, 0);
    n2 = node(1, 0, 0);
    assertTrue(NodeUtils.compare(n1, n2) > 0);

    n1 = node(1, 1, 0);
    n2 = node(1, 1, 0);
    assertTrue(NodeUtils.compare(n1, n2) == 0);

    n1 = node(1, 1);
    n2 = node(1, 1, 0);
    assertTrue(NodeUtils.compare(n1, n2) < 0);
  }

  @Test
  public void testCompareAncestor() {
    IntsRef n1 = node(0, 0, 0);
    IntsRef n2 = node(1, 0, 0);
    assertTrue(NodeUtils.compareAncestor(n1, n2) < 0);

    n1 = node(1, 1, 0);
    n2 = node(1, 0, 0);
    assertTrue(NodeUtils.compareAncestor(n1, n2) > 0);

    n1 = node(1, 1, 0);
    n2 = node(1, 1, 0);
    assertTrue(NodeUtils.compareAncestor(n1, n2) > 0);

    n1 = node(1, 1);
    n2 = node(1, 1, 0);
    assertTrue(NodeUtils.compareAncestor(n1, n2) == 0);
  }

  @Test
  public void testIsConstraintSatisfied() {
    final int[] levelIndex = new int[] { 1,3 };
    final int[][] constraints = new int[][] {
      new int[] { 1,8 },
      new int[] { 5,5 },
    };

    IntsRef node = node(1,0,5);
    assertTrue(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(8,10,5);
    assertTrue(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(4,5,5,90);
    assertTrue(NodeUtils.isConstraintSatisfied(node, 4, levelIndex, constraints));

    node = node(4);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(4,10);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(0,12,5);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(9,12,5);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(4,5,4);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));

    node = node(4,5,6);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3, levelIndex, constraints));
  }

  @Test
  public void testIsLevelConstraintSatisfied() {
    IntsRef node = node(1,5,1);
    assertTrue(NodeUtils.isConstraintSatisfied(node, 3));

    node = node(1,5,1);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 2));

    node = node(1,1,0,0);
    assertFalse(NodeUtils.isConstraintSatisfied(node, 3));
  }

}
