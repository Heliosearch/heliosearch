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
package com.sindicetech.siren.qparser.tree.dsl;

/**
 * Class that represents a descendant clause object of the JSON query syntax.
 * Compared to a {@link BasicQueryClause}, a descendant clause object has a
 * level field.
 */
class DescendantQueryClause extends QueryClause {

  private final int level;

  /**
   * Create a new descendant clause object.
   *
   * @see {@link com.sindicetech.siren.search.node.TwigQuery#addDescendant(int,
   * com.sindicetech.siren.search.node.NodeQuery,
   * com.sindicetech.siren.search.node.NodeBooleanClause.Occur)
   */
  DescendantQueryClause(final AbstractNodeQuery query, final Occur occur, final int level) {
    super(query, occur);
    this.level = level;
  }

  /**
   * Retrieve the node level
   */
  int getLevel() {
    return level;
  }

}
