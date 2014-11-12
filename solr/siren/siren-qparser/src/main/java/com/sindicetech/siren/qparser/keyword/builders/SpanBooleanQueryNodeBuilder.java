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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

import com.sindicetech.siren.qparser.keyword.nodes.SpanBooleanQueryNode;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.spans.BooleanSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;

import java.util.List;

/**
 * Builds a {@link com.sindicetech.siren.search.node.NodeBooleanQuery} object from a {@link com.sindicetech.siren.qparser.keyword.nodes.NodeBooleanQueryNode}
 * object.
 *
 * <p>
 *
 * Every children in the {@link com.sindicetech.siren.qparser.keyword.nodes.NodeBooleanQueryNode} object must be already tagged
 * using {@link org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link org.apache.lucene.search.Query}
 * object.
 *
 * <p>
 *
 * It takes in consideration if the children is a {@link org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode} to
 * define the {@link com.sindicetech.siren.search.node.NodeBooleanClause}.
 */
public class SpanBooleanQueryNodeBuilder implements KeywordQueryBuilder {

  public SpanBooleanQueryNodeBuilder() {
  }

  public NodeQuery build(final QueryNode queryNode)
  throws QueryNodeException {
    final SpanBooleanQueryNode booleanNode = (SpanBooleanQueryNode) queryNode;
    final List<QueryNode> children = booleanNode.getChildren();
    final BooleanSpanQuery bq = new BooleanSpanQuery(booleanNode.getSlop(), booleanNode.isInOrder());

    if (children == null) {
      return bq; // return empty boolean query
    }

    // If more than one child, add them into the BooleanSpanQuery
    if (children.size() > 1) {
      for (final QueryNode child : children) {
        final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
        if (obj != null) {
          if (obj instanceof SpanQuery) {
            bq.add((SpanQuery) obj, NodeQueryBuilderUtil.getModifierValue(child, NodeBooleanClause.Occur.SHOULD));
          }
          else {
            throw new QueryNodeException(
              new MessageImpl("Expected SpanQuery: got '" + obj.getClass().getCanonicalName() + "'"));
          }
        }
      }
      return bq;
    }
    // If only one child, return it directly
    else {
      final Object obj = children.get(0).getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (obj != null) {
        if (obj instanceof SpanQuery) {
          return (NodeQuery) obj;
        }
        else {
          throw new QueryNodeException(new Error("Non SpanQuery query '" +
            obj.getClass().getCanonicalName() + "' received"));
        }
      }
      return bq; // return empty boolean query
    }
  }

}
