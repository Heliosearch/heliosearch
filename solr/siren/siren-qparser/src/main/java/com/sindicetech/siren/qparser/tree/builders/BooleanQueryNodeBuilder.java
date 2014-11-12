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
package com.sindicetech.siren.qparser.tree.builders;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.tree.nodes.BooleanQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.TopLevelQueryNode;
import com.sindicetech.siren.qparser.tree.parser.InOrderPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.RangePropertyParser;
import com.sindicetech.siren.qparser.tree.parser.SlopPropertyParser;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.spans.BooleanSpanQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;

/**
 * Builds a {@link BooleanQuery} or a {@link BooleanSpanQuery} object from a {@link BooleanQueryNode} object.
 * A {@link BooleanSpanQuery} object is created if:
 * <ul>
 *   <li> the {@link BooleanQueryNode} contains a {@link SlopPropertyParser#SLOP_PROPERTY} or a
 *       {@link InOrderPropertyParser#IN_ORDER_PROPERTY} attributes
 *   <li> if the parent of the {@link BooleanQueryNode} is not a {@link TopLevelQueryNode}
 * </ul>
 * <p>
 * Every child in the {@link BooleanQueryNode} object must be already tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link NodeQuery} object.
 * <p>
 * It expects its children to be of type {@link ModifierQueryNode},and converts them into {@link NodeBooleanClause}s
 * or {@link BooleanClause}s.
 */
public class BooleanQueryNodeBuilder implements ExtendedTreeQueryBuilder {

  public static final int DEFAULT_SLOP = 0;

  public static final boolean DEFAULT_INORDER = false;

  public BooleanQueryNodeBuilder() {}

  public Query build(final QueryNode queryNode) throws QueryNodeException {
    final BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;
    if (this.isBooleanSpan(booleanNode)) {
      return this.buildBooleanSpanQuery(booleanNode);
    }
    else {
      return this.buildBooleanQuery(booleanNode);
    }
  }

  private final BooleanSpanQuery buildBooleanSpanQuery(BooleanQueryNode booleanNode) throws QueryNodeException {
    // check if the node has a slop
    int slop = this.DEFAULT_SLOP;
    if (booleanNode.getTag(SlopPropertyParser.SLOP_PROPERTY) != null) {
      slop = (Integer) booleanNode.getTag(SlopPropertyParser.SLOP_PROPERTY);
    }

    // check if the node has a inOrder flag
    boolean inOrder = this.DEFAULT_INORDER;
    if (booleanNode.getTag(InOrderPropertyParser.IN_ORDER_PROPERTY) != null) {
      int tag = (Integer) booleanNode.getTag(InOrderPropertyParser.IN_ORDER_PROPERTY);
      inOrder = tag == 0 ? false : true;
    }

    // build the query and add clauses
    final BooleanSpanQuery bsq = new BooleanSpanQuery(slop, inOrder);
    for (final QueryNode child : booleanNode.getChildren()) {
      final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      try {
        bsq.add(new NodeSpanQuery((NodeQuery) obj), this.getNodeModifierValue(child));
      }
      catch (final BooleanQuery.TooManyClauses ex) {
        throw new QueryNodeException(new MessageImpl(QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES,
          BooleanQuery.getMaxClauseCount(), booleanNode.toQueryString(new EscapeQuerySyntaxImpl())), ex);
      }
    }

    // check if the node has a node range constraint
    if (booleanNode.getTag(RangePropertyParser.RANGE_PROPERTY) != null) {
      final int[] range = (int[]) booleanNode.getTag(RangePropertyParser.RANGE_PROPERTY);
      bsq.setNodeConstraint(range[0], range[1]);
    }

    return bsq;
  }

  private final BooleanQuery buildBooleanQuery(BooleanQueryNode booleanNode) throws QueryNodeException {
    // build the query and add clauses
    final BooleanQuery bq = new BooleanQuery(true);

    for (final QueryNode child : booleanNode.getChildren()) {
      final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      try {
        Query q = (Query) obj;
        if (obj instanceof NodeQuery) {
          // wrap the query into a LuceneProxyNodeQuery
          q = new LuceneProxyNodeQuery((NodeQuery) q);
        }
        bq.add(q, this.getLuceneModifierValue(child));
      }
      catch (final BooleanQuery.TooManyClauses ex) {
        throw new QueryNodeException(new MessageImpl(QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES,
        BooleanQuery.getMaxClauseCount(), booleanNode.toQueryString(new EscapeQuerySyntaxImpl())), ex);
      }
    }

    return bq;
  }

  private final boolean isBooleanSpan(BooleanQueryNode booleanNode) {
    boolean isBooleanSpan = false;

    if (booleanNode.containsTag(SlopPropertyParser.SLOP_PROPERTY)) {
      isBooleanSpan = true;
    }
    if (booleanNode.containsTag(InOrderPropertyParser.IN_ORDER_PROPERTY)) {
      isBooleanSpan = true;
    }

    return isBooleanSpan;
  }

  private final NodeBooleanClause.Occur getNodeModifierValue(final QueryNode node) {
    if (node instanceof ModifierQueryNode) {
      final ModifierQueryNode mNode = ((ModifierQueryNode) node);
      switch (mNode.getModifier()) {
      case MOD_REQ:
        return NodeBooleanClause.Occur.MUST;

      case MOD_NOT:
        return NodeBooleanClause.Occur.MUST_NOT;

      case MOD_NONE:
        return NodeBooleanClause.Occur.SHOULD;
      }
    }
    return NodeBooleanClause.Occur.SHOULD;
  }

  private final BooleanClause.Occur getLuceneModifierValue(final QueryNode node) {
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
