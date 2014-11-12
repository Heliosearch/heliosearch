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
package com.sindicetech.siren.qparser.tree.builders.concise;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.tree.builders.TwigQueryNodeBuilder;
import com.sindicetech.siren.qparser.tree.nodes.TwigQueryNode;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.util.JSONDatatype;

/**
 * An extension of the {@link com.sindicetech.siren.qparser.tree.builders.TwigQueryNodeBuilder} for the concise model. It
 * uses a {@link com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser} to parse the root boolean expression.
 */
public class ConciseTwigQueryNodeBuilder extends TwigQueryNodeBuilder {

  public ConciseTwigQueryNodeBuilder(final ConciseKeywordQueryParser keywordParser) {
    super(keywordParser);
  }

  /**
   * Process the root query. In the concise model, the original expression is mapped to the attribute, and replaced
   * by an empty string.
   */
  @Override
  protected final void processRoot(final TwigQueryNode twigNode, final TwigQuery query) throws QueryNodeException {
    if (twigNode.hasRoot()) {
      // save the default datatype
      String defaultDatatype = keywordParser.getQueryConfigHandler().get(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DEFAULT_DATATYPE);
      // assign json:field as default datatype for the root
      keywordParser.setDefaultDatatype(JSONDatatype.JSON_FIELD);

      final String rootExpr = twigNode.getRoot().toString();
      final String field = twigNode.getField().toString();

      ((ConciseKeywordQueryParser) keywordParser).setAttribute(rootExpr);
      query.addRoot((NodeQuery) keywordParser.parse("", field));

      // restore the default datatype
      keywordParser.setDefaultDatatype(defaultDatatype);
    }
  }

}
