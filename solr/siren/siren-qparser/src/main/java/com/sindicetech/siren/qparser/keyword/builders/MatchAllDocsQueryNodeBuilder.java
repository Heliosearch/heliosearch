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

import org.apache.commons.lang.NotImplementedException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.MatchAllDocsQuery;

/**
 * Builds a {@link MatchAllDocsQuery} object from a
 * {@link MatchAllDocsQueryNode} object.
 */
public class MatchAllDocsQueryNodeBuilder implements KeywordQueryBuilder {

  public MatchAllDocsQueryNodeBuilder() {
    // empty constructor
  }

  public MatchAllDocsQuery build(QueryNode queryNode) throws QueryNodeException {
    throw new NotImplementedException("MatchAllDocsQueries are not supported yet");
    
//TODO: To implement when Siren will support MatchAllDocs queries
//    // validates node
//    if (!(queryNode instanceof MatchAllDocsQueryNode)) {
//      throw new QueryNodeException(new MessageImpl(
//          QueryParserMessages.LUCENE_QUERY_CONVERSION_ERROR, queryNode
//              .toQueryString(new EscapeQuerySyntaxImpl()), queryNode.getClass()
//              .getName()));
//    }
//
//    return new MatchAllDocsQuery();

  }

}
