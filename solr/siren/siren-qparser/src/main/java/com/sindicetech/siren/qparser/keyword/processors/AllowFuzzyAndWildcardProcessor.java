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
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;

import java.util.List;

/**
 * This processor checks if the {@link KeywordConfigurationKeys#ALLOW_FUZZY_AND_WILDCARD}
 * configuration is satisfied.
 *
 * <p>
 *
 * This processor verifies if the configuration key
 * {@link KeywordConfigurationKeys#ALLOW_FUZZY_AND_WILDCARD} is defined in the
 * {@link com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler}. If it is and if fuzzy and wildcard are not
 * allowed, it looks for every {@link FuzzyQueryNode} or
 * {@link WildcardQueryNode} contained in the query node tree and throws an
 * exception if it finds any of them.
 *
 * @see KeywordConfigurationKeys#ALLOW_FUZZY_AND_WILDCARD
 */
public class AllowFuzzyAndWildcardProcessor extends QueryNodeProcessorImpl {

  public AllowFuzzyAndWildcardProcessor() {
    // empty constructor
  }

  @Override
  public QueryNode process(final QueryNode queryTree) throws QueryNodeException {

    if (this.getQueryConfigHandler().has(KeywordConfigurationKeys.ALLOW_FUZZY_AND_WILDCARD)) {
      if (!this.getQueryConfigHandler().get(KeywordConfigurationKeys.ALLOW_FUZZY_AND_WILDCARD)) {
        return super.process(queryTree);
      }
    }
    return queryTree;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {

    if (node instanceof WildcardQueryNode) {
      throw new QueryNodeException(new MessageImpl("Wildcard not allowed", node
              .toQueryString(new EscapeQuerySyntaxImpl())));
    }


    if (node instanceof FuzzyQueryNode) {
      throw new QueryNodeException(new MessageImpl("Fuzzy not allowed", node
              .toQueryString(new EscapeQuerySyntaxImpl())));
    }

    return node;

  }

  @Override
  protected QueryNode preProcessNode(final QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}
