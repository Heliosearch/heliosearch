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

package com.sindicetech.siren.index;

import java.io.IOException;

import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.util.NodeUtils;

/**
 * Implementation of {@link ConstrainedNodesEnum} to apply a level constraint
 * and a stack of interval constraints over the retrieved node labels.
 *
 * <p>
 *
 * This {@link ConstrainedNodesEnum} applies a stack of interval constraints.
 * A stack of interval constraints is composed of a list of pairs
 * (level, interval constraint), each pair indicates one interval constraint to
 * apply on specific level of the node labels.
 *
 * @see NodeUtils#isConstraintSatisfied(IntsRef, int[], int[][])
 */
public class IntervalConstrainedNodesEnum extends ConstrainedNodesEnum {

  private final int level;
  private final int[] levelIndex;
  private final int[][] constraints;

  public IntervalConstrainedNodesEnum(final DocsNodesAndPositionsEnum docsEnum,
                                      final int level,
                                      final int[] levelIndex,
                                      final int[][] constraints) {
    super(docsEnum);
    this.level = level;
    this.levelIndex = levelIndex;
    this.constraints = constraints;
  }

  @Override
  public boolean nextNode() throws IOException {
    while (docsEnum.nextNode()) {
      if (NodeUtils.isConstraintSatisfied(docsEnum.node(), level, levelIndex, constraints)) {
        return true;
      }
    }
    return false;
  }

}
