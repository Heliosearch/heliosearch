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

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.config.ConciseTreeQueryConfigHandler;

/**
 * Extension of the {@link com.sindicetech.siren.qparser.tree.dsl.QueryBuilder} for the concise model.
 */
public class ConciseQueryBuilder extends QueryBuilder {

  public ConciseQueryBuilder() {
    this(new ConciseTreeQueryConfigHandler());
  }

  public ConciseQueryBuilder(final ConciseTreeQueryConfigHandler config) {
    super(config);
  }

  /**
   * Create a new node query with the specified boolean expression
   * <p>
   * The boolean expression must follow the syntax of the
   * {@link com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser}.
   */
  @Override
  public ConciseNodeQuery newNode(final String booleanExpression) throws QueryNodeException {
    final ConciseNodeQuery node = new ConciseNodeQuery(mapper, (ConciseKeywordQueryParser) parser);
    node.setBooleanExpression(booleanExpression);
    return node;
  }

  @Override
  public ConciseTwigQuery newTwig(final String booleanExpression) throws QueryNodeException {
    final ConciseTwigQuery twig = new ConciseTwigQuery(mapper, (ConciseKeywordQueryParser) parser);
    twig.setRoot(booleanExpression);
    return twig;
  }

}
