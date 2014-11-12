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

import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.StandardExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.processors.QNamesProcessor;

import java.util.Properties;

/**
 * This query configuration handler which is used in the
 * {@link com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser}.
 */
public class ExtendedTreeQueryConfigHandler extends QueryConfigHandler {

  final public static class ConfigurationKeys  {

    /**
     * Key used to set the {@link com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser} used for boolean clause
     * found in the query
     *
     * @see com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser#setKeywordQueryParser(com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser)
     * @see com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser#getKeywordQueryParser()
     */
    final public static ConfigurationKey<ExtendedKeywordQueryParser> KEYWORD_PARSER = ConfigurationKey.newInstance();

    /**
     * Key used to set the default boolean operator
     *
     * @see com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser#setDefaultOperator(org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator)
     * @see com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser#getDefaultOperator()
     */
    final public static ConfigurationKey<Operator> DEFAULT_OPERATOR = ConfigurationKey.newInstance();

    /**
     * Key used to set the qnames mapping
     *
     * @see QNamesProcessor
     */
    final public static ConfigurationKey<Properties> QNAMES = ConfigurationKey.newInstance();

  }

  /**
   * Create a default {@link ExtendedTreeQueryConfigHandler}.
   * <p>
   * The default configuration includes:
   * <ul>
   * <li> {@link Operator#AND} as default operator
   * <li> A {@link com.sindicetech.siren.qparser.keyword.StandardExtendedKeywordQueryParser} with the twig syntactic sugar disabled
   * and {@link Operator#AND} as default operator.
   * </ul>
   */
  public ExtendedTreeQueryConfigHandler() {
    // Set default operator
    this.set(ConfigurationKeys.DEFAULT_OPERATOR, Operator.AND);

    // Set default keyword parser
    final StandardExtendedKeywordQueryParser parser = new StandardExtendedKeywordQueryParser();
    // Disable twig queries: syntactic sugar for twig queries must be disabled
    // in the JSON parser
    parser.setAllowTwig(false);
    // set default operator
    parser.setDefaultOperator(Operator.AND);
    this.set(ConfigurationKeys.KEYWORD_PARSER, parser);
  }

}
