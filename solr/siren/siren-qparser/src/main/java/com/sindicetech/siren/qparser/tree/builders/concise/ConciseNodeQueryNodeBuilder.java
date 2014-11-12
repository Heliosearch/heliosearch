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
package com.sindicetech.siren.qparser.tree.builders.concise;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryBuilder;
import com.sindicetech.siren.qparser.tree.nodes.NodeQueryNode;
import com.sindicetech.siren.qparser.tree.parser.LevelPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.RangePropertyParser;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * An extension of the {@link com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryBuilder} for the concise model. It
 * uses a {@link com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser} to parse the boolean expression.
 */
public class ConciseNodeQueryNodeBuilder implements ExtendedTreeQueryBuilder {

  private final ConciseKeywordQueryParser keywordParser;

  public ConciseNodeQueryNodeBuilder(final ConciseKeywordQueryParser keywordParser) {
    this.keywordParser = keywordParser;
  }

  @Override
  public NodeQuery build(final QueryNode queryNode) throws QueryNodeException {
    final NodeQueryNode node = (NodeQueryNode) queryNode;
    final String field = node.getField().toString();
    final String expr = node.getValue().toString();

    // ensure that the attribute is unset
    keywordParser.unsetAttribute();

    // set attribute
    if (node.getAttribute() != null) {
      keywordParser.setAttribute(node.getAttribute().toString());
    }

    final NodeQuery query = (NodeQuery) keywordParser.parse(expr, field);

    // check if the node has a level constraint
    if (node.getTag(LevelPropertyParser.LEVEL_PROPERTY) != null) {
      query.setLevelConstraint((Integer) node.getTag(LevelPropertyParser.LEVEL_PROPERTY));
    }

    // check if the node has a node range constraint
    if (node.getTag(RangePropertyParser.RANGE_PROPERTY) != null) {
      final int[] range = (int[]) node.getTag(RangePropertyParser.RANGE_PROPERTY);
      query.setNodeConstraint(range[0], range[1]);
    }

    return query;
  }

}
