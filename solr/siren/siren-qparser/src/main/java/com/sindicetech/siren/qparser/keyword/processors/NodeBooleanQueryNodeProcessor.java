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
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.NodeBooleanQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.SpanBooleanQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;

import java.util.List;

/**
 * This processor converts a {@link BooleanQueryNode} into a {@link NodeBooleanQueryNode} or
 * {@link SpanBooleanQueryNode} at post-processing time.
 * <p>
 * If a {@link BooleanQueryNode} has a tag {@link QueryTypeProcessor#QUERYTYPE_TAG} set with the value
 * {@link QueryTypeProcessor#SPAN_QUERYTYPE}, it is converted into a {@link SpanBooleanQueryNode}.
 * <p>
 * A {@link BooleanQueryNode} is converted into a {@link NodeBooleanQueryNode} if
 * {@link KeywordConfigurationKeys#ALLOW_TWIG} is set to <code>false</code>. When it is set to <code>true</code>, only
 * those within a {@link TwigQueryNode} are converted.
 */
public class NodeBooleanQueryNodeProcessor extends QueryNodeProcessorImpl {

  /**
   * Number of twigs
   */
  private int nbTwigs = 0;

  @Override
  protected QueryNode preProcessNode(final QueryNode node) throws QueryNodeException {
    if (node instanceof TwigQueryNode) {
      nbTwigs++;
    }
    return node;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {
    QueryNode newNode = node;

    if (node instanceof TwigQueryNode) {
      nbTwigs--;
      assert nbTwigs >= 0;
    }
    else if (node instanceof BooleanQueryNode) {
      /*
       * - if tagged with the span query type, always convert it to a SpanBooleanQueryNode
       * - if twig is allowed, convert it to a NodeBooleanQueryNode only if within a twig querynode
       * - if twig is not allowed, convert it to a NodeBooleanQueryNode
       */
      if (node.getTag(QueryTypeProcessor.QUERYTYPE_TAG) == QueryTypeProcessor.SPAN_QUERYTYPE) {
        newNode = new SpanBooleanQueryNode((BooleanQueryNode) node);
      }
      else if (this.getQueryConfigHandler().has(KeywordConfigurationKeys.ALLOW_TWIG)) {
        if (!(this.getQueryConfigHandler().get(KeywordConfigurationKeys.ALLOW_TWIG))) {
          newNode = new NodeBooleanQueryNode((BooleanQueryNode) node);
        }
        else if (nbTwigs > 0) {
          newNode = new NodeBooleanQueryNode((BooleanQueryNode) node);
        }
      }
      else {
        throw new IllegalArgumentException("KeywordConfigurationKeys.ALLOW_TWIG should be set on the ExtendedKeywordQueryConfigHandler");
      }
    }

    return newNode;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
  throws QueryNodeException {
    return children;
  }

}
