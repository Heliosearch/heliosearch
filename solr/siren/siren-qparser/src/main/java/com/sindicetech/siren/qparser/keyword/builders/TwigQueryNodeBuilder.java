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
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.nodes.ArrayQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.WildcardNodeQueryNode;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause.Occur;

/**
 * Builds a {@link TwigQuery} from a {@link TwigQueryNode}. Both the root
 * and the child must be already tagged using a
 * {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query} object.
 *
 * <p>
 *
 * It takes in consideration if a value is a {@link ModifierQueryNode} to
 * define its {@link NodeBooleanClause}.
 */
public class TwigQueryNodeBuilder
implements StandardQueryBuilder {

  public TwigQueryNodeBuilder() {}

  @Override
  public Query build(final QueryNode queryNode)
  throws QueryNodeException {
    final TwigQueryNode tqn = (TwigQueryNode) queryNode;
    final QueryNode root = tqn.getRoot();
    final QueryNode child = tqn.getChild();
    final TwigQuery twigQuery;
    final int rootLevel = tqn.getRootLevel();

    if (root == null && child == null) {
      throw new QueryNodeException(new MessageImpl(QueryParserMessages.EMPTY_MESSAGE));
    }
    if (tqn.getChildren().size() != 2) {
      throw new IllegalArgumentException("A TwigQueryNode cannot have more " +
          "than 2 children:\n" + tqn.getChildren().toString());
    }
    if (child instanceof WildcardNodeQueryNode &&
        root instanceof WildcardNodeQueryNode) {
      throw new QueryNodeException(new MessageImpl("Twig with both root and " +
          "child empty is not allowed."));
    }
    // Build the root operand
    if (root instanceof WildcardNodeQueryNode) { // Empty root query
      twigQuery = new TwigQuery(rootLevel);
    } else {
      final Object attQuery = root.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (attQuery != null) {
        twigQuery = new TwigQuery(rootLevel);
        twigQuery.addRoot((NodeQuery) attQuery);
      } else {
        throw new QueryNodeException(new MessageImpl(QueryParserMessages.INVALID_SYNTAX,
        "Unable to get the root of the Twig query"));
      }
    }
    if (!(child instanceof WildcardNodeQueryNode)) {
      // Build the child operand
      final Object v = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (v instanceof ArrayQuery) { // array of children nodes
        final ArrayQueryNode aqn = (ArrayQueryNode) child;
        final List<Query> children = ((ArrayQuery) v).getElements();
        for (int i = 0; i < children.size(); i++) {
          twigQuery.addChild((NodeQuery) children.get(i),
            NodeQueryBuilderUtil.getModifierValue(aqn.getChildren().get(i), NodeBooleanClause.Occur.MUST));
        }
      } else if (v instanceof Query) {
        final NodeQuery valQuery = (NodeQuery) v;
        twigQuery.addChild(valQuery, Occur.MUST);
      } else {
        throw new QueryNodeException(new MessageImpl(QueryParserMessages.INVALID_SYNTAX,
          "Unexpected class of a Twig Query clause: " + v == null ? "null" : v.getClass().getName()));
      }
    }

    return twigQuery;
  }

}
