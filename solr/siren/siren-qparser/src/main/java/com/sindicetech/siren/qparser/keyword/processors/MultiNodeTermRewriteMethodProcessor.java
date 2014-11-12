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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.AbstractRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.MultiTermRewriteMethodProcessor;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;

/**
 * This processor instates the default
 * {@link com.sindicetech.siren.search.node.MultiNodeTermQuery.RewriteMethod},
 * {@link MultiNodeTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}, for multi-term
 * query nodes.
 *
 * <p>
 *
 * Copied from {@link MultiTermRewriteMethodProcessor} and modified for the
 * SIREn use case.
 */
public class MultiNodeTermRewriteMethodProcessor extends QueryNodeProcessorImpl {

  public static final String TAG_ID = "MultiNodeTermRewriteMethodConfiguration";

  @Override
  protected QueryNode postProcessNode(final QueryNode node) {

    // set setMultiTermRewriteMethod for WildcardQueryNode and
    // PrefixWildcardQueryNode
    if (node instanceof WildcardQueryNode ||
        node instanceof AbstractRangeQueryNode ||
        node instanceof RegexpQueryNode) {

      final MultiNodeTermQuery.RewriteMethod rewriteMethod = this.getQueryConfigHandler().get(KeywordConfigurationKeys.MULTI_NODE_TERM_REWRITE_METHOD);

      if (rewriteMethod == null) {
        // This should not happen, this configuration is set in the
        // StandardQueryConfigHandler
        throw new IllegalArgumentException(
            "KeywordConfigurationKeys.MULTI_NODE_TERM_REWRITE_METHOD should be set on the QueryConfigHandler");
      }

      // use a TAG to take the value to the Builder
      node.setTag(MultiNodeTermRewriteMethodProcessor.TAG_ID, rewriteMethod);

    }

    return node;
  }

  @Override
  protected QueryNode preProcessNode(final QueryNode node) {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children) {
    return children;
  }
}
