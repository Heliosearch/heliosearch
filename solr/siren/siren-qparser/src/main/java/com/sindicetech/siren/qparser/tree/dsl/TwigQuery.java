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
package com.sindicetech.siren.qparser.tree.dsl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.tree.dsl.QueryClause.Occur;
import com.sindicetech.siren.qparser.tree.parser.*;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.util.JSONDatatype;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that represents a twig object of the JSON query syntax.
 */
public class TwigQuery extends AbstractNodeQuery {

  protected boolean hasRoot = false;
  protected String rootBooleanExpression;

  protected final List<QueryClause> clauses;

  protected final ExtendedKeywordQueryParser parser;

  public TwigQuery(final ObjectMapper mapper, final ExtendedKeywordQueryParser parser) {
    super(mapper);
    this.parser = parser;
    clauses = new ArrayList<QueryClause>();
  }

  @Override
  public TwigQuery setLevel(final int level) {
    return (TwigQuery) super.setLevel(level);
  }

  @Override
  public TwigQuery setRange(final int lowerBound, final int upperBound) {
    return (TwigQuery) super.setRange(lowerBound, upperBound);
  }

  @Override
  public TwigQuery setBoost(final float boost) {
    return (TwigQuery) super.setBoost(boost);
  }

  /**
   * Set the node boolean query expression for the root of the twig.
   *
   * @see com.sindicetech.siren.search.node.TwigQuery#addRoot(NodeQuery)
   */
  void setRoot(final String booleanExpression) {
    this.rootBooleanExpression = booleanExpression;
    this.hasRoot = true;
  }

  /**
   * Adds a child clause with a
   * {@link NodeBooleanClause.Occur#MUST} operator.
   * <p>
   * Use this method for child clauses that must appear in the matching twigs.
   *
   * @see NodeBooleanClause.Occur#MUST
   * @see com.sindicetech.siren.search.node.TwigQuery#addChild(NodeQuery, NodeBooleanClause.Occur)
   */
  public TwigQuery with(final AbstractNodeQuery child) {
    clauses.add(new BasicQueryClause(child, Occur.MUST));
    return this;
  }

  /**
   * Adds a descendant clause with a
   * {@link NodeBooleanClause.Occur#MUST} operator.
   * <p>
   * Use this method for descendant clauses that must appear in the matching
   * twigs.
   *
   * @see NodeBooleanClause.Occur#MUST
   * @see com.sindicetech.siren.search.node.TwigQuery#addDescendant(int, NodeQuery, NodeBooleanClause.Occur)
   */
  public TwigQuery with(final AbstractNodeQuery descendant, final int level) {
    clauses.add(new DescendantQueryClause(descendant, Occur.MUST, level));
    return this;
  }

  /**
   * Adds a child clause with a
   * {@link NodeBooleanClause.Occur#MUST_NOT} operator.
   * <p>
   * Use this method for child clauses that must not appear in the matching
   * twigs.
   *
   * @see NodeBooleanClause.Occur#MUST_NOT
   * @see com.sindicetech.siren.search.node.TwigQuery#addChild(NodeQuery, NodeBooleanClause.Occur)
   */
  public TwigQuery without(final AbstractNodeQuery child) {
    clauses.add(new BasicQueryClause(child, Occur.MUST_NOT));
    return this;
  }

  /**
   * Adds a descendant clause with a
   * {@link NodeBooleanClause.Occur#MUST_NOT} operator.
   * <p>
   * Use this method for descendant clauses that must not appear in the matching
   * twigs.
   *
   * @see NodeBooleanClause.Occur#MUST_NOT
   * @see com.sindicetech.siren.search.node.TwigQuery#addDescendant(int, NodeQuery, NodeBooleanClause.Occur)
   */
  public TwigQuery without(final AbstractNodeQuery descendant, final int level) {
    clauses.add(new DescendantQueryClause(descendant, Occur.MUST_NOT, level));
    return this;
  }

  /**
   * Adds a child clause with a
   * {@link NodeBooleanClause.Occur#SHOULD} operator.
   * <p>
   * Use this method for child clauses that should appear in the matching
   * twigs.
   *
   * @see NodeBooleanClause.Occur#SHOULD
   * @see com.sindicetech.siren.search.node.TwigQuery#addChild(NodeQuery, NodeBooleanClause.Occur)
   */
  public TwigQuery optional(final AbstractNodeQuery child) {
    clauses.add(new BasicQueryClause(child, Occur.SHOULD));
    return this;
  }

  /**
   * Adds a descendant clause with a
   * {@link NodeBooleanClause.Occur#SHOULD} operator.
   * <p>
   * Use this method for descendant clauses that should appear in the matching
   * twigs.
   *
   * @see NodeBooleanClause.Occur#SHOULD
   * @see com.sindicetech.siren.search.node.TwigQuery#addDescendant(int, NodeQuery, NodeBooleanClause.Occur)
   */
  public TwigQuery optional(final AbstractNodeQuery descendant, final int level) {
    clauses.add(new DescendantQueryClause(descendant, Occur.SHOULD, level));
    return this;
  }

  @Override
  public Query toQuery(final boolean proxy) throws QueryNodeException {
    final com.sindicetech.siren.search.node.TwigQuery query = new com.sindicetech.siren.search.node.TwigQuery();
    // parse and add root
    this.processRoot(query);
    // convert child and descendant clauses
    for (final QueryClause clause : clauses) {
      if (clause instanceof BasicQueryClause) {
        query.addChild((NodeQuery) clause.getQuery().toQuery(false), clause.getNodeBooleanOccur());
      }
      else {
        final int level = ((DescendantQueryClause) clause).getLevel();
        query.addDescendant(level, (NodeQuery) clause.getQuery().toQuery(false), clause.getNodeBooleanOccur());
      }
    }
    // add level
    if (this.hasLevel()) {
      query.setLevelConstraint(this.getLevel());
    }
    // add range
    if (this.hasRange()) {
      query.setNodeConstraint(this.getLowerBound(), this.getUpperBound());
    }
    // add boost
    if (this.hasBoost()) {
      query.setBoost(this.getBoost());
    }

    // should we wrap the query into a lucene proxy
    if (proxy) {
      return new LuceneProxyNodeQuery(query);
    }
    return query;
  }

  /**
   * Parses the root boolean expression and add it to the twig query.
   */
  protected void processRoot(final com.sindicetech.siren.search.node.TwigQuery query) throws QueryNodeException {
    if (hasRoot) {
      // save default datatype
      String defaultDatatype = parser.getQueryConfigHandler().get(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DEFAULT_DATATYPE);
      // assign json:field as default datatype for the root
      parser.setDefaultDatatype(JSONDatatype.JSON_FIELD);

      query.addRoot((NodeQuery) parser.parse(rootBooleanExpression, ""));

      // restore the default datatype
      parser.setDefaultDatatype(defaultDatatype);
    }
  }

  @Override
  public ObjectNode toJson() {
    final ObjectNode obj = mapper.createObjectNode();
    final ObjectNode twig = obj.putObject(TwigPropertyParser.TWIG_PROPERTY);

    if (hasRoot) {
      twig.put(RootPropertyParser.ROOT_PROPERTY, rootBooleanExpression);
    }

    if (this.hasLevel()) {
      twig.put(LevelPropertyParser.LEVEL_PROPERTY, this.getLevel());
    }

    if (this.hasRange()) {
      final ArrayNode array = twig.putArray(RangePropertyParser.RANGE_PROPERTY);
      array.add(this.getLowerBound());
      array.add(this.getUpperBound());
    }

    if (this.hasBoost()) {
      twig.put(BoostPropertyParser.BOOST_PROPERTY, this.getBoost());
    }

    ArrayNode childArray = null;
    ArrayNode descendantArray = null;
    for (final QueryClause clause : clauses) {
      if (clause instanceof BasicQueryClause) {
        if (!twig.has(ChildPropertyParser.CHILD_PROPERTY)) { // avoid to create an empty array in the JSON
          childArray = twig.putArray(ChildPropertyParser.CHILD_PROPERTY);
        }
        final ObjectNode e = childArray.addObject();
        e.put(OccurPropertyParser.OCCUR_PROPERTY, clause.getOccur().toString());
        e.putAll(clause.getQuery().toJson());
      }
      else {
        if (!twig.has(DescendantPropertyParser.DESCENDANT_PROPERTY)) { // avoid to create an empty array in the JSON
          descendantArray = twig.putArray(DescendantPropertyParser.DESCENDANT_PROPERTY);
        }
        final ObjectNode e = descendantArray.addObject();
        e.put(OccurPropertyParser.OCCUR_PROPERTY, clause.getOccur().toString());
        e.put(LevelPropertyParser.LEVEL_PROPERTY, ((DescendantQueryClause) clause).getLevel());
        e.putAll(clause.getQuery().toJson());
      }
    }

    return obj;
  }



}
