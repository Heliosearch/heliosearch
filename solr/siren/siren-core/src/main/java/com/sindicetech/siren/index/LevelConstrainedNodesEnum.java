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

import com.sindicetech.siren.util.NodeUtils;

/**
 * Implementation of {@link ConstrainedNodesEnum} to apply a level constraint
 * over the retrieved node labels.
 *
 * @see NodeUtils#isConstraintSatisfied(int[], int[], int[], boolean)
 */
public class LevelConstrainedNodesEnum extends ConstrainedNodesEnum {

  private final int level;

  public LevelConstrainedNodesEnum(final DocsNodesAndPositionsEnum docsEnum,
                                   final int level) {
    super(docsEnum);
    this.level = level;
  }

  @Override
  public boolean nextNode() throws IOException {
    while (docsEnum.nextNode()) {
      if (NodeUtils.isConstraintSatisfied(docsEnum.node(), level)) {
        return true;
      }
    }
    return false;
  }

}
