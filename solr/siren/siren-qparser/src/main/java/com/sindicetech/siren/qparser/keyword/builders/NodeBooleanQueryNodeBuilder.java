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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.NodeBooleanQueryNode;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * Builds a {@link NodeBooleanQuery} object from a {@link NodeBooleanQueryNode}
 * object.
 *
 * <p>
 *
 * Every children in the {@link NodeBooleanQueryNode} object must be already tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query}
 * object.
 *
 * <p>
 *
 * It takes in consideration if the children is a {@link ModifierQueryNode} to
 * define the {@link NodeBooleanClause}.
 */
public class NodeBooleanQueryNodeBuilder implements KeywordQueryBuilder {

  public NodeBooleanQueryNodeBuilder() {
  }

  public NodeQuery build(final QueryNode queryNode)
  throws QueryNodeException {
    final NodeBooleanQueryNode booleanNode = (NodeBooleanQueryNode) queryNode;
    final List<QueryNode> children = booleanNode.getChildren();
    final NodeBooleanQuery bq = new NodeBooleanQuery();

    if (children == null) {
      return bq; // return empty boolean query
    }

    // If more than one child, wrap them into a NodeBooleanQuery
    if (children.size() > 1) {
      for (final QueryNode child : children) {
        final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
        if (obj != null) {
          if (obj instanceof NodeQuery) {
            final QueryNode mod;
            if (child instanceof DatatypeQueryNode) {
              mod = ((DatatypeQueryNode) child).getChild();
            } else {
              mod = child;
            }
            bq.add((NodeQuery) obj,
              NodeQueryBuilderUtil.getModifierValue(mod, NodeBooleanClause.Occur.SHOULD));
          }
          else {
            throw new QueryNodeException(new Error("Expected NodeQuery: got '" +
            	obj.getClass().getCanonicalName() + "'"));
          }
        }
      }
      return bq;
    }
    // If only one child, return it directly
    else {
      final Object obj = children.get(0).getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (obj != null) {
        if (obj instanceof NodeQuery) {
          return (NodeQuery) obj;
        }
        else {
          throw new QueryNodeException(new Error("Non NodeQuery query '" +
            obj.getClass().getCanonicalName() + "' received"));
        }
      }
      return bq; // return empty boolean query
    }
  }

}
