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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;

import java.util.Map;
import java.util.Properties;

/**
 * An abstraction over the query parser for the keyword query syntax.
 */
public abstract class ExtendedKeywordQueryParser extends StandardQueryParser {

  /**
   * Sets the boolean operator of the QueryParser. In default mode (
   * {@link Operator#OR}) terms without any modifiers are considered optional:
   * for example <code>capital of Hungary</code> is equal to
   * <code>capital OR of OR Hungary</code>.<br/>
   * In {@link Operator#AND} mode terms are considered to be in conjunction: the
   * above mentioned query is parsed as <code>capital AND of AND Hungary</code>
   * <p>
   * Default: {@link Operator#AND}
   */
  public void setDefaultOperator(final Operator operator) {
    if (operator == Operator.OR) {
      super.setDefaultOperator(Operator.OR);
    } else {
      super.setDefaultOperator(Operator.AND);
    }
  }

  /**
   * Set the set of qnames to URI mappings.
   * <p>
   * Default: <code>null</code>.
   */
  public void setQNames(final Properties qnames) {
    this.getQueryConfigHandler().set(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.QNAMES, qnames);
  }

  /**
   * Set to <code>true</code> to allow fuzzy and wildcard queries.
   * <p>
   * Default: <code>true</code>.
   */
  public void setAllowFuzzyAndWildcard(final boolean allowFuzzyWildcard) {
    this.getQueryConfigHandler().set(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.ALLOW_FUZZY_AND_WILDCARD, allowFuzzyWildcard);
  }

  /**
   * Set the {@link Map} that associates a datatype to its {@link Analyzer}.
   * <p>
   * Default:
   * <pre>
   *     map.put(XSDDatatype.XSD_STRING, new StandardAnalyzer(Version.LUCENE_40));
   *     map.put(JSONDatatype.JSON_FIELD, new WhitespaceAnalyzer(Version.LUCENE_40));
   * </pre>
   */
  public void setDatatypeAnalyzers(final Map<String, Analyzer> dtAnalyzers) {
    this.getQueryConfigHandler().set(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DATATYPES_ANALYZERS, dtAnalyzers);
  }

  /**
   * Set the default datatype that will be used when no datatype is defined.
   * <p>
   * Default: XSDDatatype.XSD_STRING
   */
  public void setDefaultDatatype(String defaultDatatype) {
    this.getQueryConfigHandler().set(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.DEFAULT_DATATYPE, defaultDatatype);
  }

  @Override
  public void setAnalyzer(final Analyzer analyzer) {
    throw new IllegalAccessError("Use #setDatatypesAnalyzers instead.");
  }

  /**
   * Set the default root level of a {@link com.sindicetech.siren.search.node.TwigQuery}
   * <p>
   * Default: 1
   */
  public void setRootLevel(final int level) {
    this.getQueryConfigHandler().set(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.ROOT_LEVEL, level);
  }

  /**
   * Enable or not {@link com.sindicetech.siren.search.node.TwigQuery}
   * <p>
   * Default: <code>true</code>
   */
  @Deprecated
  public void setAllowTwig(final boolean allowTwig) {
    this.getQueryConfigHandler().set(ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys.ALLOW_TWIG, allowTwig);
  }

}
