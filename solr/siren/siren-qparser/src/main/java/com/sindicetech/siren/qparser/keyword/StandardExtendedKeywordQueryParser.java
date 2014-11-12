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
package com.sindicetech.siren.qparser.keyword;

import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.builders.KeywordQueryTreeBuilder;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.keyword.processors.KeywordQueryNodeProcessorPipeline;

/**
 * The standard implementation of the query parser for the keyword query syntax.
 *
 * <p>
 *
 * To construct a {@link Query} object from a query string, use the
 * {@link #parse(String, String)} method:
 * <pre>
 * ExtendedKeywordQueryParser queryParser = new ExtendedKeywordQueryParser(); <br/>
 * Query query = queryParser.parse("a : b", "sirenField");
 * </pre>
 *
 * To change any configuration before parsing the query string do, for example:
 * <pre>
 * queryParser.getQueryConfigHandler().setDefaultOperator(Operator.AND);
 * </pre>
 *
 * or use the setter methods:
 * <pre>
 *   queryParser.setDefaultOperator(Operator.AND);
 * </pre>
 *
 * <p>
 *
 * Examples of appropriately formatted queries can be found in the <a
 * href="{@docRoot}/com/sindicetech/siren/qparser/keyword/package-summary.html#package_description">
 * query syntax documentation</a>.
 *
 * <p>
 *
 * The text parser used by this helper is a {@link KeywordSyntaxParser}. The
 * BNF grammar of this parser is available <a href="{@docRoot}/../jjdoc/KeywordSyntaxParser.html">here</a>.
 *
 * <p>
 *
 * The {@link QueryConfigHandler} used by this helper is a
 * {@link com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler}.
 *
 * <p>
 *
 * The query node processor used by this helper is a
 * {@link KeywordQueryNodeProcessorPipeline}.
 *
 * <p>
 *
 * The builder used by this query parser is a {@link KeywordQueryTreeBuilder}.
 *
 * @see com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler
 * @see KeywordSyntaxParser
 * @see KeywordQueryNodeProcessorPipeline
 * @see KeywordQueryTreeBuilder
 */
public class StandardExtendedKeywordQueryParser extends ExtendedKeywordQueryParser {

  /**
   * Constructs a {@link StandardExtendedKeywordQueryParser} object.
   */
  public StandardExtendedKeywordQueryParser() {
    super();
    this.setSyntaxParser(new KeywordSyntaxParser());
    this.setQueryConfigHandler(new ExtendedKeywordQueryConfigHandler());
    this.setQueryNodeProcessor(new KeywordQueryNodeProcessorPipeline(this.getQueryConfigHandler()));
    this.setQueryBuilder(new KeywordQueryTreeBuilder());
  }

  @Override
  public String toString(){
    return "<StandardExtendedKeywordQueryParser config=\"" + this.getQueryConfigHandler() + "\"/>";
  }

}
