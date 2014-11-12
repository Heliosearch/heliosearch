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
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import com.sindicetech.siren.qparser.keyword.nodes.SpanGroupQueryNode;

import java.util.List;

/**
 * This processor tags at pre-processing the child of a {@link SpanGroupQueryNode} with the span slop and order.
 */
public class SpanGroupQueryProcessor extends QueryNodeProcessorImpl {

  /**
   * This tag is used to set the slop on that span group query.
   */
  public static final String SLOP_TAG = SpanGroupQueryProcessor.class.getName() + "-SLOP";

  /**
   * This tag is used to set the order on that span group query.
   */
  public static final String INORDER_TAG = SpanGroupQueryProcessor.class.getName() + "-ORDER";

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    final QueryConfigHandler conf = this.getQueryConfigHandler();

    // If the current node is a span group query node, transfer its slop to its child
    if (node instanceof SpanGroupQueryNode) {
      SpanGroupQueryNode spanGroupQueryNode = (SpanGroupQueryNode) node;
      // transfer the slop to its child
      spanGroupQueryNode.getChild().setTag(SLOP_TAG, spanGroupQueryNode.getSlop());
      // transfer the slop to its child
      spanGroupQueryNode.getChild().setTag(INORDER_TAG, spanGroupQueryNode.isInOrder());
    }

    return node;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children) throws QueryNodeException {
    return children;
  }

}
