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
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.search.node.NodePhraseQuery;

/**
 * This builder basically reads the {@link Query} object set on the
 * {@link SlopQueryNode} child using
 * {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} and applies the slop value
 * defined in the {@link SlopQueryNode}.
 */
public class SlopQueryNodeBuilder implements KeywordQueryBuilder {

  public SlopQueryNodeBuilder() {
  // empty constructor
  }

  public NodePhraseQuery build(QueryNode queryNode) throws QueryNodeException {
    final SlopQueryNode phraseSlopNode = (SlopQueryNode) queryNode;

    if (phraseSlopNode.getValue() != 0)
      throw new NotImplementedException("Slop Queries not supported in Siren yet");

    return (NodePhraseQuery) phraseSlopNode.getChild().getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

    // TODO: To implement when siren will support slop queries
    // SlopQueryNode phraseSlopNode = (SlopQueryNode) queryNode;
    //
    // Query query = (Query) phraseSlopNode.getChild().getTag(
    // QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
    //
    //    
    // if (query instanceof PhraseQuery) {
    // ((PhraseQuery) query).setSlop(phraseSlopNode.getValue());
    //
    // } else {
    // ((MultiPhraseQuery) query).setSlop(phraseSlopNode.getValue());
    // }
    //
    // return query;

  }

}
