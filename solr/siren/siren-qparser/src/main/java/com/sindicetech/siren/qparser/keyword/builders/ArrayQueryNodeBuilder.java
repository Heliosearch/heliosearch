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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.nodes.ArrayQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.WildcardNodeQueryNode;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.TwigQuery;

/**
 * Builds an {@link ArrayQuery} from the children of a {@link ArrayQueryNode}.
 *
 * <p>
 *
 * Nested {@link ArrayQuery}s are transformed into a {@link TwigQuery} with
 * {@link WildcardNodeQueryNode} as the root (i.e., no root constraint).
 */
public class ArrayQueryNodeBuilder
implements StandardQueryBuilder {

  @Override
  public Query build(final QueryNode queryNode)
  throws QueryNodeException {
    final ArrayQueryNode arrayNode = (ArrayQueryNode) queryNode;
    final List<QueryNode> children = arrayNode.getChildren();
    final ArrayQuery arrayQuery = new ArrayQuery();

    for (final QueryNode child : children) {
      final Object v = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (v == null) { // DummyNode such as the EmptyNodeQueryNode
        continue;
      }
      if (v instanceof Query) {
        if (v instanceof ArrayQuery) {
          /*
           * Nested array query. It is transformed as a TwigQuery with empty root
           */
          final TwigQuery twigQuery = new TwigQuery();
          for (final Query qn : ((ArrayQuery) v).getElements()) {
            final NodeQuery valQuery = (NodeQuery) qn;
            twigQuery.addChild(valQuery, NodeQueryBuilderUtil.getModifierValue(child, NodeBooleanClause.Occur.MUST));
          }
          arrayQuery.addElement(twigQuery);
        } else {
          arrayQuery.addElement((NodeQuery) v);
        }
      } else {
        throw new QueryNodeException(new MessageImpl(QueryParserMessages.INVALID_SYNTAX,
          "Unexpected class of a Twig Query clause: " + v == null ? "null" : v.getClass().getName()));
      }
    }
    return arrayQuery;
  }

}
