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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.builders.MatchAllDocsQueryNodeBuilder;
import com.sindicetech.siren.search.node.NodePhraseQuery;

/**
 * This processor throws an exception if it encounters a {@link Query}
 * that is not supported in SIREn.
 *
 * <p>
 *
 * Such queries are:
 * <ul>
 * <li>{@link NodePhraseQuery} with slop different to 0</li>
 * <li>{@link MultiPhraseQuery}</li>
 * <li>{@link MatchAllDocsQuery}</li>
 * </ul>
 */
public class NotSupportedQueryProcessor
extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode preProcessNode(final QueryNode node)
  throws QueryNodeException {
    if (node instanceof SlopQueryNode && ((SlopQueryNode) node).getValue() != 0) {
      throw new QueryNodeException(new MessageImpl("Slop queries are not supported",
        node.toQueryString(new EscapeQuerySyntaxImpl())));
    } else if (node instanceof MultiPhraseQueryNode) {
      throw new QueryNodeException(new MessageImpl("Multi phrase queries are not supported",
        node.toQueryString(new EscapeQuerySyntaxImpl())));
    } else if (node instanceof MatchAllDocsQueryNodeBuilder) {
      throw new QueryNodeException(new MessageImpl("MatchAllDocsQueries are not supported",
        node.toQueryString(new EscapeQuerySyntaxImpl())));
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
