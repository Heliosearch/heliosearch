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
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;

import java.util.List;

/**
 * This processor sets the level of the root of the {@link TwigQueryNode}.
 *
 * @see TwigQueryNode#setRootLevel(int)
 */
public class RootLevelTwigQueryNodeProcessor
extends QueryNodeProcessorImpl {

  private final int DEFAULT_ROOT = 1;

  @Override
  protected QueryNode preProcessNode(final QueryNode node) throws QueryNodeException {
    if (node instanceof TwigQueryNode) {
      final int root;
      if (this.getQueryConfigHandler().has(KeywordConfigurationKeys.ROOT_LEVEL)) {
        root = this.getQueryConfigHandler().get(KeywordConfigurationKeys.ROOT_LEVEL);
      }
      else {
        root = DEFAULT_ROOT;
      }
      // Set the ROOT level of the query
      /*
       * When adding nested TwigQueries, their root level is set to the current
       * Twig level + 1. See {@link TwigQuery#addChild}.
       */
      final TwigQueryNode twigNode = (TwigQueryNode) node;
      twigNode.setRootLevel(root);
    }
    return node;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node)
  throws QueryNodeException {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
  throws QueryNodeException {
    return children;
  }

}
