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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.builders.concise.ConciseKeywordQueryTreeBuilder;
import com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.keyword.processors.KeywordQueryNodeProcessorPipeline;

/**
 * An extension of the {@link StandardExtendedKeywordQueryParser} that introduces
 * the concept of attribute label from the concise model. The attribute label will be prepended to all the query terms
 * of the parsed query.
 *
 * <p>
 *
 * To set the attribute before parsing the query string do:
 * <pre>
 * queryParser.setAttribute("name");
 * </pre>
 *
 * <p>
 *
 * The builder used by this query parser is a {@link com.sindicetech.siren.qparser.keyword.builders.concise.ConciseKeywordQueryTreeBuilder}.
 *
 * @see StandardExtendedKeywordQueryParser
 * @see com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler
 * @see com.sindicetech.siren.qparser.keyword.builders.concise.ConciseKeywordQueryTreeBuilder
 */
public class ConciseKeywordQueryParser extends ExtendedKeywordQueryParser {

  /**
   * Constructs a {@link ConciseKeywordQueryParser} object.
   */
  public ConciseKeywordQueryParser() {
    super();
    this.setSyntaxParser(new KeywordSyntaxParser());
    this.setQueryConfigHandler(new ConciseKeywordQueryConfigHandler());
    this.setQueryNodeProcessor(new KeywordQueryNodeProcessorPipeline(this.getQueryConfigHandler()));
    this.setQueryBuilder(new ConciseKeywordQueryTreeBuilder(this.getQueryConfigHandler()));
  }

  /**
   * Set the attribute to prepend to the query terms.
   */
  public void setAttribute(final String attribute) {
    this.getQueryConfigHandler().set(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE, attribute);
  }

  /**
   * Unset the attribute to prepend to the query terms.
   */
  public void unsetAttribute() {
    this.getQueryConfigHandler().unset(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE);
  }

  @Override
  public String toString(){
    return "<ConciseKeywordQueryParser config=\"" + this.getQueryConfigHandler() + "\"/>";
  }

  @Override
  public Query parse(String query, String defaultField) throws QueryNodeException {
    ((ConciseKeywordQueryTreeBuilder) getQueryBuilder()).setDefaultField(defaultField);
    return super.parse(query, defaultField);
  }

}
