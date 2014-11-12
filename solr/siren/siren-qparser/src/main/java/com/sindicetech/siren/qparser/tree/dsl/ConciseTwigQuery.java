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
import org.codehaus.jackson.map.ObjectMapper;

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.util.JSONDatatype;

/**
 * Extension of the {@link com.sindicetech.siren.qparser.tree.dsl.TwigQuery} for the concise model that
 * modifies how the root boolean expression is converted.
 */
public class ConciseTwigQuery extends TwigQuery {

  public ConciseTwigQuery(final ObjectMapper mapper, final ConciseKeywordQueryParser parser) {
    super(mapper, parser);
  }

  /**
   * Parses the root boolean expression and add it to the twig query.
   */
  @Override
  protected void processRoot(final com.sindicetech.siren.search.node.TwigQuery query) throws QueryNodeException {
    if (hasRoot) {
      // save default datatype
      String defaultDatatype = parser.getQueryConfigHandler().get(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DEFAULT_DATATYPE);
      // assign json:field as default datatype for the root
      parser.setDefaultDatatype(JSONDatatype.JSON_FIELD);

      ((ConciseKeywordQueryParser) parser).setAttribute(rootBooleanExpression);
      query.addRoot((com.sindicetech.siren.search.node.NodeQuery) parser.parse("", ""));

      // restore the default datatype
      parser.setDefaultDatatype(defaultDatatype);
    }
  }

}
