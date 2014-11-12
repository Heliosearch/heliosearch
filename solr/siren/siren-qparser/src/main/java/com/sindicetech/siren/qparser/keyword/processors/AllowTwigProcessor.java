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
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;

import java.util.List;

/**
 * This processor checks if the {@link KeywordConfigurationKeys#ALLOW_TWIG}
 * configuration is satisfied.
 *
 * <p>
 *
 * This processor verifies if the configuration key
 * {@link KeywordConfigurationKeys#ALLOW_TWIG} is defined in the
 * {@link com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler}. If it is and twig is not allowed,
 * it looks for every {@link TwigQueryNode} contained in the query node tree
 * and throws an exception if it finds any.
 *
 * @see KeywordConfigurationKeys#ALLOW_TWIG
 */
public class AllowTwigProcessor
extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode preProcessNode(final QueryNode node) throws QueryNodeException {
    if (node instanceof TwigQueryNode) {
      if (this.getQueryConfigHandler().has(KeywordConfigurationKeys.ALLOW_TWIG)) {
        if (!this.getQueryConfigHandler().get(KeywordConfigurationKeys.ALLOW_TWIG)) {
          throw new QueryNodeException(new MessageImpl("TwigQuery not allowed", node
            .toQueryString(new EscapeQuerySyntaxImpl())));
        }
      }
      else {
        throw new IllegalArgumentException("KeywordConfigurationKeys.ALLOW_TWIG should be set on the ExtendedKeywordQueryConfigHandler");
      }
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
