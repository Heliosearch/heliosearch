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
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;

/**
 * Builds a {@link BooleanQuery} object from a {@link BooleanQueryNode} object.
 *
 * <p>
 *
 * Every children in the {@link BooleanQueryNode} object must be already tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query}
 * object. It takes in consideration if the children is a
 * {@link ModifierQueryNode} to define the {@link BooleanClause}.
 */
public class BooleanQueryNodeBuilder implements StandardQueryBuilder {

  public BooleanQueryNodeBuilder() {
    // empty constructor
  }

  public Query build(final QueryNode queryNode) throws QueryNodeException {
    final BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;

    final BooleanQuery bQuery = new BooleanQuery();
    final List<QueryNode> children = booleanNode.getChildren();

    if (children != null) {

      for (final QueryNode child : children) {
        final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

        if (obj != null) {
          final Query query = (Query) obj;

          try {
            final QueryNode mod;
            if (child instanceof DatatypeQueryNode) {
              mod = ((DatatypeQueryNode) child).getChild();
            } else {
              mod = child;
            }
            bQuery.add(query, getModifierValue(mod));

          } catch (final TooManyClauses ex) {

            throw new QueryNodeException(new MessageImpl(
                QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES, BooleanQuery
                    .getMaxClauseCount(), queryNode
                    .toQueryString(new EscapeQuerySyntaxImpl())), ex);

          }

        }

      }

    }

    return bQuery;

  }

  private static BooleanClause.Occur getModifierValue(final QueryNode node) {

    if (node instanceof ModifierQueryNode) {
      final ModifierQueryNode mNode = ((ModifierQueryNode) node);
      switch (mNode.getModifier()) {

      case MOD_REQ:
        return BooleanClause.Occur.MUST;

      case MOD_NOT:
        return BooleanClause.Occur.MUST_NOT;

      case MOD_NONE:
        return BooleanClause.Occur.SHOULD;

      }

    }

    return BooleanClause.Occur.SHOULD;

  }

}
