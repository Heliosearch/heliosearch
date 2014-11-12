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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.PhraseQueryNodeBuilder;

import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.search.node.NodePhraseQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;

import java.util.List;

/**
 * Builds a {@link NodePhraseQuery} object from a {@link TokenizedPhraseQueryNode}
 * object.
 *
 * <p>
 *
 * Code taken from {@link PhraseQueryNodeBuilder} and adapted for SIREn
 */
public class NodePhraseQueryNodeBuilder implements KeywordQueryBuilder {

  public NodePhraseQueryNodeBuilder() {}

  public NodePhraseQuery build(final QueryNode queryNode) throws QueryNodeException {
    final TokenizedPhraseQueryNode phraseNode = (TokenizedPhraseQueryNode) queryNode;

    final NodePhraseQuery phraseQuery = new NodePhraseQuery();
    final List<QueryNode> children = phraseNode.getChildren();

    if (children != null) {
      for (QueryNode child : children) {
        // the query node should always be a FieldQueryNode
        assert child instanceof FieldQueryNode;
        final FieldQueryNode termNode = (FieldQueryNode) child;
        final NodeTermQuery termQuery = (NodeTermQuery) child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
        phraseQuery.add(termQuery.getTerm(), termNode.getPositionIncrement());
      }
    }
    // assign the datatype. We must always have a datatype assigned.
    phraseQuery.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));
    return phraseQuery;
  }

}
