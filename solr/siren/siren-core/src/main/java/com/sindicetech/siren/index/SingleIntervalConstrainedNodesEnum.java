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
 * and an interval constraint over the retrieved node labels.
 *
 * @see NodeUtils#isConstraintSatisfied(IntsRef, int[], int[][])
 */
public class SingleIntervalConstrainedNodesEnum extends ConstrainedNodesEnum {

  private final int level;
  private final int[] constraint;

  public SingleIntervalConstrainedNodesEnum(final DocsNodesAndPositionsEnum docsEnum,
                                            final int level,
                                            final int[] constraint) {
    super(docsEnum);
    this.level = level;
    this.constraint = constraint;
  }

  @Override
  public boolean nextNode() throws IOException {
    while (docsEnum.nextNode()) {
      if (NodeUtils.isConstraintSatisfied(docsEnum.node(), level, constraint)) {
        return true;
      }
    }
    return false;
  }

}
