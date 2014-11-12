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
package com.sindicetech.siren.qparser.tree.config;

import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;

/**
 * This query configuration handler which is used by the {@link com.sindicetech.siren.qparser.tree.ConciseTreeQueryParser}.
 */
public class ConciseTreeQueryConfigHandler extends ExtendedTreeQueryConfigHandler {

  /**
   * Create a default {@link ConciseTreeQueryConfigHandler}.
   * <p>
   * The default configuration includes:
   * <ul>
   * <li> {@link org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator#AND} as default operator
   * <li> A {@link com.sindicetech.siren.qparser.keyword.StandardExtendedKeywordQueryParser} with the twig syntactic sugar disabled
   * and {@link org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator#AND} as default operator.
   * </ul>
   */
  public ConciseTreeQueryConfigHandler() {
    // Set default operator
    this.set(ConfigurationKeys.DEFAULT_OPERATOR, Operator.AND);

    // Set default concise keyword parser
    final ConciseKeywordQueryParser parser = new ConciseKeywordQueryParser();
    // set default operator
    parser.setDefaultOperator(Operator.AND);
    this.set(ConfigurationKeys.KEYWORD_PARSER, parser);
  }

}
