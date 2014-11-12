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
package com.sindicetech.siren.qparser.tree.builders;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.tree.nodes.TopLevelQueryNode;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * Check if the child in the {@link TopLevelQueryNode} object is a
 * {@link org.apache.lucene.search.BooleanQuery} or a {@link NodeQuery}. If it is a {@link NodeQuery},
 * wraps it into a {@link LuceneProxyNodeQuery}.
 * <p>
 * The child in the {@link TopLevelQueryNode} object must be tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a
 * {@link Query} object.
 */
public class TopLevelQueryNodeBuilder implements ExtendedTreeQueryBuilder {

  public TopLevelQueryNodeBuilder() {}

  @Override
  public Query build(final QueryNode queryNode) throws QueryNodeException {
    final TopLevelQueryNode topNode = (TopLevelQueryNode) queryNode;
    final QueryNode child = topNode.getChildren().get(0);
    final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

    if (obj instanceof NodeQuery) {
      return new LuceneProxyNodeQuery((NodeQuery) obj);
    }
    else {
      // no need to wrap the query object into a lucene proxy query
      return (Query) obj;
    }
  }

}
