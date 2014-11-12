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
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;

import com.sindicetech.siren.qparser.keyword.nodes.NodeGroupQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.SpanGroupQueryNode;

import java.util.List;

/**
 * This processor tags at pre-processing all the descendants of
 * {@link com.sindicetech.siren.qparser.keyword.nodes.NodeGroupQueryNode} or
 * {@link com.sindicetech.siren.qparser.keyword.nodes.SpanGroupQueryNode} with the {@link #QUERYTYPE_TAG} tag to indicate
 * the query type of the query node.
 */
public class QueryTypeProcessor extends QueryNodeProcessorImpl {

  /**
   * This tag is used to set the query type to be used on that query node.
   */
  public static final String QUERYTYPE_TAG = QueryTypeProcessor.class.getName() + "-TYPE";

  /**
   * This value is used to specify the node query type.
   */
  public static final String NODE_QUERYTYPE = "NODE";

  /**
   * This value is used to specify the span query type.
   */
  public static final String SPAN_QUERYTYPE = "SPAN";

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    // If the current node is a group query node, check the query type and assign it to its children
    if (node instanceof GroupQueryNode) {
      GroupQueryNode groupQueryNode = (GroupQueryNode) node;
      String queryType = null;

      if (node instanceof NodeGroupQueryNode) {
        queryType = NODE_QUERYTYPE;
      }
      else if (node instanceof SpanGroupQueryNode) {
        queryType = SPAN_QUERYTYPE;
      }
      else {
        throw new QueryNodeException(new MessageImpl("Invalid GroupQueryNode received",
          node.toQueryString(new EscapeQuerySyntaxImpl())));
      }

      // transfer the query type to its child
      groupQueryNode.getChild().setTag(QUERYTYPE_TAG, queryType);
    }
    // in any other cases, if the node is not a leaf node, transfer the query type to its children
    else if (!node.isLeaf()) {
      if (node.getTag(QUERYTYPE_TAG) != null) {
        for (final QueryNode child : node.getChildren()) {
          child.setTag(QUERYTYPE_TAG, node.getTag(QUERYTYPE_TAG));
        }
      }
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
