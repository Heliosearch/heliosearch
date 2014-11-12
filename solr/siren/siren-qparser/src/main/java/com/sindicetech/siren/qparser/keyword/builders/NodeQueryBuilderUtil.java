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
package com.sindicetech.siren.qparser.keyword.builders;

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * Set of utility methods for building a {@link NodeQuery}.
 * 
 * @see TwigQueryNodeBuilder
 * @see ArrayQueryNodeBuilder
 * @see NodeBooleanQueryNodeBuilder
 */
class NodeQueryBuilderUtil {

  /**
   * Returns the {@link NodeBooleanClause.Occur} that corresponds to the node
   * modifier.
   *
   * @param node the {@link ModifierQueryNode}
   * @param def the default {@link NodeBooleanClause.Occur} to return
   *            if the node is not a {@link ModifierQueryNode}
   * @return the {@link NodeBooleanClause.Occur} of the query node
   */
  static NodeBooleanClause.Occur getModifierValue(final QueryNode node,
                                                  final NodeBooleanClause.Occur def) {
    if (node instanceof ModifierQueryNode) {
      final ModifierQueryNode mNode = ((ModifierQueryNode) node);
      switch (mNode.getModifier()) {
        case MOD_REQ:
          return NodeBooleanClause.Occur.MUST;
        case MOD_NOT:
          return NodeBooleanClause.Occur.MUST_NOT;
        case MOD_NONE:
          return NodeBooleanClause.Occur.SHOULD;
      }
    }
    return def;
  }

}
