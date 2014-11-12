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
package com.sindicetech.siren.qparser.keyword.config;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.FieldDateResolutionFCListener;
import org.apache.lucene.queryparser.flexible.standard.config.FuzzyConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.util.Version;

import com.sindicetech.siren.qparser.keyword.processors.*;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.search.node.MultiNodeTermQuery.RewriteMethod;
import com.sindicetech.siren.util.JSONDatatype;
import com.sindicetech.siren.util.XSDDatatype;

import java.util.*;

/**
 * This is used to configure parameters for the {@link KeywordQueryNodeProcessorPipeline}.
 *
 * <p>
 *
 * Copied from {@link StandardQueryConfigHandler} and adapted to SIREn.
 */
public class ExtendedKeywordQueryConfigHandler extends QueryConfigHandler {

  /**
   * Class holding the {@link KeywordQueryNodeProcessorPipeline} options.
   */
  final public static class KeywordConfigurationKeys {

    /**
     * Key used to set the qnames mapping.
     * @see QNamesProcessor
     */
    final public static ConfigurationKey<Properties> QNAMES = ConfigurationKey.newInstance();

    /**
     * Key used to set if {@link TwigQuery}s are allowed.
     * @see AllowTwigProcessor
     */
    final public static ConfigurationKey<Boolean> ALLOW_TWIG = ConfigurationKey.newInstance();

    /**
     * Key used to set if fuzzy and wildcard queries are allowed.
     * @see AllowFuzzyAndWildcardProcessor
     */
    final public static ConfigurationKey<Boolean> ALLOW_FUZZY_AND_WILDCARD = ConfigurationKey.newInstance();

    /**
     * Key used to set the default root level of a {@link TwigQuery}. The level of
     * nested TwigQueries increments with the default root level as offset.
     * @see RootLevelTwigQueryNodeProcessor
     */
    final public static ConfigurationKey<Integer> ROOT_LEVEL = ConfigurationKey.newInstance();

    /**
     * Key used to set the pair of Datatype, e.g., {@link XSDDatatype#XSD_STRING},
     * and its associated {@link Analyzer}.
     * @see com.sindicetech.siren.qparser.keyword.processors.DatatypeProcessor
     */
    final public static ConfigurationKey<Map<String, Analyzer>> DATATYPES_ANALYZERS = ConfigurationKey.newInstance();

    /**
     * Key used to set the default datatype, e.g., {@link XSDDatatype#XSD_STRING}
     * @see com.sindicetech.siren.qparser.keyword.processors.DatatypeProcessor
     */
    final public static ConfigurationKey<String> DEFAULT_DATATYPE = ConfigurationKey.newInstance();

    /**
     * Key used to set the {@link RewriteMethod} used when creating queries
     *
     * @see com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser#setMultiTermRewriteMethod(org.apache.lucene.search.MultiTermQuery.RewriteMethod)
     * @see com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser#getMultiTermRewriteMethod()
     */
    final public static ConfigurationKey<MultiNodeTermQuery.RewriteMethod> MULTI_NODE_TERM_REWRITE_METHOD = ConfigurationKey.newInstance();

  }

  public ExtendedKeywordQueryConfigHandler() {
    // Add listener that will build the FieldConfig attributes.
    // TODO: setting field configuration is to be deprecated once datatypes
    // in SIREn are correctly handled.
    this.addFieldConfigListener(new FieldDateResolutionFCListener(this));

    // Default Values
    this.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, false); // default in 2.9
    this.set(ConfigurationKeys.ANALYZER, null); //default value 2.4
    this.set(ConfigurationKeys.PHRASE_SLOP, 0); //default value 2.4
    this.set(ConfigurationKeys.LOWERCASE_EXPANDED_TERMS, false); // we should not lowercase expanded terms (#66)
    this.set(ConfigurationKeys.FIELD_BOOST_MAP, new LinkedHashMap<String, Float>());
    this.set(ConfigurationKeys.FUZZY_CONFIG, new FuzzyConfig());
    this.set(ConfigurationKeys.LOCALE, Locale.getDefault());
    this.set(ConfigurationKeys.FIELD_DATE_RESOLUTION_MAP, new HashMap<CharSequence, DateTools.Resolution>());

    // SIREn Default Values
    // This key is not used in SIREn. Instead, we use DATATYPES_ANALYZERS
    this.set(ConfigurationKeys.ANALYZER, null);
    // Set the primitive datatypes. Those are mandatory.
    final Map<String, Analyzer> datatypes = new HashMap<String, Analyzer>();
    datatypes.put(XSDDatatype.XSD_STRING, new StandardAnalyzer(Version.LUCENE_46));
    datatypes.put(JSONDatatype.JSON_FIELD, new WhitespaceAnalyzer(Version.LUCENE_46));
    this.set(KeywordConfigurationKeys.DATATYPES_ANALYZERS, datatypes);
    // The default datatype to assign if no datatype is defined
    this.set(KeywordConfigurationKeys.DEFAULT_DATATYPE, XSDDatatype.XSD_STRING);

    this.set(KeywordConfigurationKeys.ALLOW_TWIG, true);
    this.set(ConfigurationKeys.ENABLE_POSITION_INCREMENTS, true);
    this.set(KeywordConfigurationKeys.ALLOW_FUZZY_AND_WILDCARD, true);
    this.set(ConfigurationKeys.DEFAULT_OPERATOR, Operator.AND);
    this.set(KeywordConfigurationKeys.MULTI_NODE_TERM_REWRITE_METHOD, MultiNodeTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    this.set(KeywordConfigurationKeys.ROOT_LEVEL, 1);
  }

}
