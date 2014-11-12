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

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.config.ExtendedTreeQueryConfigHandler;
import com.sindicetech.siren.qparser.tree.config.ExtendedTreeQueryConfigHandler.ConfigurationKeys;

/**
 * This class is a helper that enables users to easily build SIREn's JSON
 * queries.
 * <p>
 * More information about the JSON query syntax can be found in
 * {@link com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser}.
 * <p>
 * The builder enables the creation of {@link AbstractQuery} which can then be
 * converted into a JSON representation using the method
 * {@link AbstractQuery#toString()} or into a {@link Query} using the method
 * {@link AbstractQuery#toQuery(boolean)}.
 *
 * @see com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser
 */
public class QueryBuilder {

  protected final ExtendedKeywordQueryParser parser;

  protected final ObjectMapper mapper;

  public QueryBuilder() {
    this(new ExtendedTreeQueryConfigHandler());
  }

  public QueryBuilder(final ExtendedTreeQueryConfigHandler config) {
    this.parser = config.get(ConfigurationKeys.KEYWORD_PARSER);
    this.mapper = new ObjectMapper();
  }

  /**
   * Create a new node query with the specified boolean expression
   * <p>
   * The boolean expression must follow the syntax of the
   * {@link com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser}.
   */
  public NodeQuery newNode(final String booleanExpression) throws QueryNodeException {
    final NodeQuery node = new NodeQuery(mapper, parser);
    node.setBooleanExpression(booleanExpression);
    return node;
  }

  /**
   * Create a new boolean query.
   */
  public AbstractBooleanQuery newBoolean() {
    return new BooleanQuery(mapper);
  }

  /**
   * Create a new twig query with empty root
   */
  public TwigQuery newTwig() {
    return new TwigQuery(mapper, parser);
  }

  /**
   * Create a new twig query with the specified boolean expression as root query
   * <p>
   * The boolean expression must follow the syntax of the
   * {@link com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser}.
   */
  public TwigQuery newTwig(final String booleanExpression) throws QueryNodeException {
    final TwigQuery twig = new TwigQuery(mapper, parser);
    twig.setRoot(booleanExpression);
    return twig;
  }

}
