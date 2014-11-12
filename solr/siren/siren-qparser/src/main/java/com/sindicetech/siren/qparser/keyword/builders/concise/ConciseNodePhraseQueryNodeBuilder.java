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
package com.sindicetech.siren.qparser.keyword.builders.concise;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;

import com.sindicetech.siren.qparser.keyword.builders.NodePhraseQueryNodeBuilder;
import com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler;
import com.sindicetech.siren.search.node.NodePhraseQuery;

import java.util.List;

/**
 * An extension of the {@link com.sindicetech.siren.qparser.keyword.builders.NodePhraseQueryNodeBuilder} that will prepend
 * the {@link com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys#ATTRIBUTE} to the encoded value
 * of the {@link org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode} before delegating the construction
 * of the {@link com.sindicetech.siren.search.node.NodeQuery} to the original
 * {@link com.sindicetech.siren.qparser.keyword.builders.NodePhraseQueryNodeBuilder}.
 */
public class ConciseNodePhraseQueryNodeBuilder extends NodePhraseQueryNodeBuilder {

  private final QueryConfigHandler conf;

  private final StringBuilder builder = new StringBuilder();

  public ConciseNodePhraseQueryNodeBuilder(final QueryConfigHandler queryConf) {
    this.conf = queryConf;
  }

  @Override
  public NodePhraseQuery build(final QueryNode queryNode) throws QueryNodeException {
    final String attribute = conf.get(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE);

    final TokenizedPhraseQueryNode phraseNode = (TokenizedPhraseQueryNode) queryNode;
    final List<QueryNode> children = phraseNode.getChildren();

    if (children != null) {
      for (QueryNode child : children) {
        // the query node should always be a FieldQueryNode
        assert child instanceof FieldQueryNode;
        final FieldQueryNode termNode = (FieldQueryNode) child;
        // Prepend the attribute to the phrase query term
        ConciseNodeBuilderUtil.prepend(builder, attribute, termNode);
      }
    }

    return super.build(phraseNode);
  }

}
