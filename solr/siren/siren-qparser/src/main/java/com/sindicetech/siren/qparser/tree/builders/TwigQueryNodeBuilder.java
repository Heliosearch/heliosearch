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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.tree.nodes.ChildQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.DescendantQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.TwigQueryNode;
import com.sindicetech.siren.qparser.tree.parser.LevelPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.RangePropertyParser;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.util.JSONDatatype;

import java.util.List;

/**
 * Builds a {@link TwigQuery} object from a {@link TwigQueryNode} object.
 * <p>
 * Every children in the {@link TwigQueryNode} object must be already tagged
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a
 * {@link NodeQuery} object.
 * <p>
 * The root of a twig query will be assigned {@link com.sindicetech.siren.util.JSONDatatype#JSON_FIELD} as default datatype.
 * <p>
 * It takes in consideration if the children is a {@link ChildQueryNode} or
 * a {@link DescendantQueryNode} to define the clauses of the {@link TwigQuery}
 * object.
 * <p>
 * Relies on a {@link com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser} object to convert the root's node
 * boolean expression into a {@link NodeQuery}.
 */
public class TwigQueryNodeBuilder implements ExtendedTreeQueryBuilder {

  protected final ExtendedKeywordQueryParser keywordParser;

  public TwigQueryNodeBuilder(final ExtendedKeywordQueryParser keywordParser) {
    this.keywordParser = keywordParser;
  }

  @Override
  public TwigQuery build(final QueryNode queryNode) throws QueryNodeException {
    final TwigQueryNode twigNode = (TwigQueryNode) queryNode;
    final List<QueryNode> children = twigNode.getChildren();
    final TwigQuery query = new TwigQuery();

    // check if the node has a level constraint
    if (twigNode.getTag(LevelPropertyParser.LEVEL_PROPERTY) != null) {
      query.setLevelConstraint((Integer) twigNode.getTag(LevelPropertyParser.LEVEL_PROPERTY));
    }

    // check if the node has a node range constraint
    if (twigNode.getTag(RangePropertyParser.RANGE_PROPERTY) != null) {
      final int[] range = (int[]) twigNode.getTag(RangePropertyParser.RANGE_PROPERTY);
      query.setNodeConstraint(range[0], range[1]);
    }

    // process root query
    this.processRoot(twigNode, query);

    // process child and descendant queries
    try {
      this.processChildren(children, query);
    }
    catch (final TooManyClauses ex) {
      throw new QueryNodeException(new MessageImpl(
          QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES,
          BooleanQuery.getMaxClauseCount(),
          twigNode.toQueryString(new EscapeQuerySyntaxImpl())), ex);
    }

    return query;
  }

  /**
   * Process the root query
   */
  protected void processRoot(final TwigQueryNode twigNode, final TwigQuery query) throws QueryNodeException {
    if (twigNode.hasRoot()) {
      // save the default datatype
      String defaultDatatype = keywordParser.getQueryConfigHandler().get(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DEFAULT_DATATYPE);
      // assign json:field as default datatype for the root
      keywordParser.setDefaultDatatype(JSONDatatype.JSON_FIELD);

      final String rootExpr = twigNode.getRoot().toString();
      final String field = twigNode.getField().toString();
      query.addRoot((NodeQuery) keywordParser.parse(rootExpr, field));

      // restore the default datatype
      keywordParser.setDefaultDatatype(defaultDatatype);
    }
  }

  /**
   * Process the child and descendant queries
   */
  protected final void processChildren(final List<QueryNode> children, final TwigQuery query) {
    for (final QueryNode child : children) {
      final Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      final NodeQuery nodeQuery = (NodeQuery) obj;

      // Append child queries
      if (child instanceof ChildQueryNode) {
        query.addChild(nodeQuery, this.getModifierValue(child));
      }
      // Append descendant queries
      else if (child instanceof DescendantQueryNode) {
        // A descendant node must always have a level constraint
        if (!child.containsTag(LevelPropertyParser.LEVEL_PROPERTY)) {
          throw new IllegalArgumentException("Invalid DescendantQueryNode received: no level constraint defined");
        }
        // set level constraint
        final int nodeLevel = (Integer) child.getTag(LevelPropertyParser.LEVEL_PROPERTY);
        // add descendant query
        query.addDescendant(nodeLevel, nodeQuery, this.getModifierValue(child));
      }
      else {
        throw new IllegalArgumentException("Invalid QueryNode received: " + child.getClass().getSimpleName());
      }
    }
  }

  protected final NodeBooleanClause.Occur getModifierValue(final QueryNode node) {
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

}
