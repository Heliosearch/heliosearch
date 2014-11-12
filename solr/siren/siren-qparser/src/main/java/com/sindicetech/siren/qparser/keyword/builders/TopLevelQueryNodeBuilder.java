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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.TopLevelQueryNode;
import com.sindicetech.siren.qparser.keyword.processors.TopLevelQueryNodeProcessor;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * Visits each node in a {@link TopLevelQueryNode} and wraps each
 * {@link NodeQuery} object tagged with
 * {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} into a
 * {@link LuceneProxyNodeQuery}.
 *
 * <p>
 *
 * This builder is called at the last step of the query building. If the key
 * {@link KeywordConfigurationKeys#ALLOW_TWIG} is <code>false</code>, this
 * builder will not be called because it is removed from the {@link QueryNode}
 * tree (see {@link TopLevelQueryNodeProcessor}).
 */
public class TopLevelQueryNodeBuilder implements KeywordQueryBuilder {

  @Override
  public Query build(final QueryNode queryNode) throws QueryNodeException {
    final TopLevelQueryNode top = (TopLevelQueryNode) queryNode;
    final Query q = (Query) top.getChildren().get(0)
                               .getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

    return this.wrap(q);
  }

  /**
   * Wraps a {@link NodeQuery} into a {@link LuceneProxyNodeQuery}.
   * This method is applied on each clause of a {@link BooleanQuery}.
   */
  protected Query wrap(final Query q) throws QueryNodeException {
    if (q instanceof BooleanQuery) {
      for (final BooleanClause clause: ((BooleanQuery) q).clauses()) {
        final Query cq = clause.getQuery();
        clause.setQuery(this.wrap(cq));
      }
      return q;
    }
    else if (q instanceof NodeQuery) {
      return new LuceneProxyNodeQuery((NodeQuery) q);
    }
    else {
      throw new QueryNodeException(new Error("Expected a BooleanQuery or a NodeQuery: got '" +
      q.getClass().getCanonicalName() + "'"));
    }
  }

}
