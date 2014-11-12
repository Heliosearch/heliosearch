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

package com.sindicetech.siren.qparser.keyword.processors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.TopLevelQueryNode;

/**
 * This processor removes the {@link TopLevelQueryNode} and returns its
 * child wrapped in a {@link BooleanQueryNode}, in the case where
 * {@link KeywordConfigurationKeys#ALLOW_TWIG} is <code>false</code>; otherwise,
 * it is left unchanged.
 */
public class TopLevelQueryNodeProcessor implements QueryNodeProcessor {

  private QueryConfigHandler queryConfig;

  @Override
  public QueryNode process(final QueryNode queryTree) throws QueryNodeException {
    final TopLevelQueryNode top = (TopLevelQueryNode) queryTree;

    if (this.getQueryConfigHandler().has(KeywordConfigurationKeys.ALLOW_TWIG)) {
      if (!this.getQueryConfigHandler().get(KeywordConfigurationKeys.ALLOW_TWIG)) {
        // Wraps the children into a BooleanQueryNode, so that the parent
        // pointers are correct.
        // This relies on the BooleanSingleChildOptimizationQueryNodeProcessor
        return new BooleanQueryNode(top.getChildren());
      }
    }
    else {
      throw new IllegalArgumentException("KeywordConfigurationKeys.ALLOW_TWIG should be set on the ExtendedKeywordQueryConfigHandler");
    }
    return queryTree;
  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)}.
   *
   * @param queryConfigHandler
   *          the query configuration handler to be set.
   *
   * @see QueryNodeProcessor#getQueryConfigHandler()
   * @see QueryConfigHandler
   */
  public void setQueryConfigHandler(final QueryConfigHandler queryConfigHandler) {
    this.queryConfig = queryConfigHandler;
  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#getQueryConfigHandler()}.
   *
   * @return QueryConfigHandler the query configuration handler to be set.
   *
   * @see QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)
   * @see QueryConfigHandler
   */
  public QueryConfigHandler getQueryConfigHandler() {
    return this.queryConfig;
  }

}
