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
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.Query;

/**
 * This builder basically reads the {@link Query} object set on the
 * {@link BoostQueryNode} child using
 * {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} and applies the boost value
 * defined in the {@link BoostQueryNode}.
 */
public class BoostQueryNodeBuilder implements KeywordQueryBuilder {

  public BoostQueryNodeBuilder() {}

  public Query build(final QueryNode queryNode) throws QueryNodeException {
    final BoostQueryNode boostNode = (BoostQueryNode) queryNode;
    final QueryNode child = boostNode.getChild();

    if (child == null) {
      return null;
    }

    final Query query = (Query) child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
    query.setBoost(boostNode.getValue());

    return query;
  }

}
