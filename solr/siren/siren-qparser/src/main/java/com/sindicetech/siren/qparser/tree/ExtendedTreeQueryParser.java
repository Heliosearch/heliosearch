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
package com.sindicetech.siren.qparser.tree;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryParserHelper;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryTreeBuilder;
import com.sindicetech.siren.qparser.tree.config.ExtendedTreeQueryConfigHandler;
import com.sindicetech.siren.qparser.tree.config.ExtendedTreeQueryConfigHandler.ConfigurationKeys;
import com.sindicetech.siren.qparser.tree.parser.JsonSyntaxParser;
import com.sindicetech.siren.qparser.tree.processors.JsonQueryNodeProcessorPipeline;

/**
 * This class is a helper that enables users to easily use the SIREn's Extended tree
 * query parser.
 *
 * <p>
 *
 * To construct a {@link Query} object from a query string, use the
 * {@link #parse(String, String)} method:
 * <pre>
 * ExtendedTreeQueryParser queryParser = new ExtendedTreeQueryParser();
 * Query query = ExtendedTreeQueryParser.parse("{ \"node\" : { \"query\" : \"aaa\" }}", "defaultField");
 * </pre>
 *
 * <p>
 *
 * To change any configuration before parsing the query string do, for example:
 * <pre>
 * // the query config handler returned by {@link ExtendedTreeQueryParser} is a {@link com.sindicetech.siren.qparser.tree.config.ExtendedTreeQueryConfigHandler}
 * queryParser.getQueryConfigHandler().setDefaultOperator(Operator.AND);
 * </pre>
 *
 * <p>
 *
 * Examples of appropriately formatted queries can be found in the <a
 * href="{@docRoot}/com/sindicetech/siren/qparser/json/package-summary.html#package_description">
 * query syntax documentation</a>.
 *
 * <p>
 *
 * The text parser used by this helper is a {@link JsonSyntaxParser}.
 *
 * <p>
 *
 * The query node processor used by this helper is a
 * {@link JsonQueryNodeProcessorPipeline}.
 *
 * <p>
 *
 * The builder used by this helper is a {@link com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryTreeBuilder}.
 *
 * @see com.sindicetech.siren.qparser.tree.config.ExtendedTreeQueryConfigHandler
 * @see JsonSyntaxParser
 * @see JsonQueryNodeProcessorPipeline
 * @see com.sindicetech.siren.qparser.tree.builders.ExtendedTreeQueryTreeBuilder
 */
public class ExtendedTreeQueryParser extends QueryParserHelper {

  public ExtendedTreeQueryParser(ExtendedTreeQueryConfigHandler queryConfigHandler, JsonSyntaxParser syntaxParser,
                                 JsonQueryNodeProcessorPipeline processor, ExtendedTreeQueryTreeBuilder builder) {
    super(queryConfigHandler, syntaxParser, processor, builder);

    // ensure that the default operator of the syntax parser is in synch
    JsonSyntaxParser jsonSyntaxParser = (JsonSyntaxParser) this.getSyntaxParser();
    jsonSyntaxParser.setDefaultModifier(this.getDefaultModifier());

    // ensure that the default operator of the keyword parser is in synch
    final ExtendedKeywordQueryParser keywordParser = this.getKeywordQueryParser();
    keywordParser.setDefaultOperator(this.getDefaultOperator());

    // configure builders with the keyword parser
    final ExtendedTreeQueryTreeBuilder jsonBuilder = (ExtendedTreeQueryTreeBuilder) this.getQueryBuilder();
    jsonBuilder.setBuilders(keywordParser);
  }

  public ExtendedTreeQueryParser() {
    this(new ExtendedTreeQueryConfigHandler(), new JsonSyntaxParser(),
         new JsonQueryNodeProcessorPipeline(null), new ExtendedTreeQueryTreeBuilder(null));
  }

  /**
   * Overrides {@link QueryParserHelper#parse(String, String)} so it casts the
   * return object to {@link Query}. For more reference about this method, check
   * {@link QueryParserHelper#parse(String, String)}.
   *
   * @param query
   *          the query string
   * @param defaultField
   *          the default field used by the text parser
   *
   * @return the object built from the query
   *
   * @throws ParseException
   *           if something wrong happens during the query parsing
   */
  @Override
  public Query parse(final String query, final String defaultField) throws QueryNodeException {
    try {
      return (Query) super.parse(query, defaultField);
    }
    catch (final QueryNodeException e) {
      throw new ParseException("Query parsing failed", e);
    }
  }

  public ModifierQueryNode.Modifier getDefaultModifier() {
    return this.getDefaultOperator() == Operator.AND ?
      ModifierQueryNode.Modifier.MOD_REQ : ModifierQueryNode.Modifier.MOD_NONE;
  }

  /**
   * Gets implicit operator setting, which will be either {@link Operator#AND}
   * or {@link Operator#OR}.
   */
  public Operator getDefaultOperator() {
    return this.getQueryConfigHandler().get(ConfigurationKeys.DEFAULT_OPERATOR);
  }

  /**
   * Sets the boolean operator of the QueryParser. In default mode (
   * {@link Operator#OR}) terms without any modifiers are considered optional:
   * for example <code>capital of Hungary</code> is equal to
   * <code>capital OR of OR Hungary</code>.<br/>
   * In {@link Operator#AND} mode terms are considered to be in conjunction: the
   * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
   */
  public void setDefaultOperator(final Operator operator) {
    this.getQueryConfigHandler().set(ConfigurationKeys.DEFAULT_OPERATOR, operator);

    // ensure that the default operator of the keyword parser is in synch
    final ExtendedKeywordQueryParser keywordParser = this.getKeywordQueryParser();
    keywordParser.setDefaultOperator(this.getDefaultOperator());
  }

  public ExtendedKeywordQueryParser getKeywordQueryParser() {
    return this.getQueryConfigHandler().get(ConfigurationKeys.KEYWORD_PARSER);
  }

  /**
   * Set the keyword query parser that will be used to parse boolean expressions
   * found in the JSON query objects.
   */
  public void setKeywordQueryParser(final ExtendedKeywordQueryParser keywordParser) {
    // ensure that the default operator of the keyword parser is in synch
    keywordParser.setDefaultOperator(this.getDefaultOperator());

    // set keyword query parser
    this.getQueryConfigHandler().set(ConfigurationKeys.KEYWORD_PARSER, keywordParser);

    // configure builders with the new keyword parser
    final ExtendedTreeQueryTreeBuilder builder = (ExtendedTreeQueryTreeBuilder) this.getQueryBuilder();
    builder.setBuilders(keywordParser);
  }

  @Override
  public void setQueryConfigHandler(final QueryConfigHandler config) {
    super.setQueryConfigHandler(config);

    // ensure that the default operator of the syntax parser is in synch
    JsonSyntaxParser syntaxParser = (JsonSyntaxParser) this.getSyntaxParser();
    syntaxParser.setDefaultModifier(this.getDefaultModifier());

    // ensure that the default operator of the keyword parser is in synch
    final ExtendedKeywordQueryParser keywordParser = this.getKeywordQueryParser();
    keywordParser.setDefaultOperator(this.getDefaultOperator());

    // configure builders with the keyword parser
    final ExtendedTreeQueryTreeBuilder builder = (ExtendedTreeQueryTreeBuilder) this.getQueryBuilder();
    builder.setBuilders(keywordParser);
  }

}
