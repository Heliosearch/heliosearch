/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sindicetech.siren.solr.qparser;

import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.solr.schema.Datatype;
import com.sindicetech.siren.solr.schema.ExtendedJsonField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * The {@link SirenQParser} is in charge of parsing a SIREn query request.
 * <p>
 * Expand the query to multiple fields by constructing a disjunction of the
 * parsed query across the fields.
 * <p>
 * For each <code>nested</code> parameter in the request, its argument
 * is parsed as a subquery and added to the main query.
 * <p>
 * The default operator for use by the query parsers is {@link Operator#AND}. It
 * can be overwritten using the parameter {@link QueryParsing#OP}.
 */
public abstract class SirenQParser extends QParser {

  protected boolean allowLeadingWildcard;
  protected Properties qnames;

  private static final Logger logger = LoggerFactory.getLogger(SirenQParser.class);

  public SirenQParser(final String qstr, final SolrParams localParams,
                      final SolrParams params, final SolrQueryRequest req) {
    super(qstr, localParams, params, req);

    // effectively disable max clauses on boolean query (heliosearch specific)
    NodeBooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    TwigQuery.setMaxClauseCount(Integer.MAX_VALUE);
  }

  /**
   * Set the QNames mapping for use in the query parser.
   */
  public void setQNames(final Properties qnames) {
    this.qnames = qnames;
  }

  /**
   * Enable or disable leading wildcard
   */
  public void setAllowLeadingWildcard(final boolean allowLeadingWildcard) {
    this.allowLeadingWildcard = allowLeadingWildcard;
  }

  @Override
  public Query parse() throws SyntaxError {
    final SolrParams solrParams = SolrParams.wrapDefaults(localParams, params);
    final Map<String, Float> boosts = parseQueryFields(req.getSchema(), solrParams);

    // We disable the coord because this query is an artificial construct
    final BooleanQuery query = new BooleanQuery(true);
    // if empty main query, ignore and try to parse the nested queries
    if (qstr != null && !qstr.isEmpty()) {
      this.processMainQuery(query, boosts, qstr);
    }
    this.processNestedQuery(query, solrParams);

    return query;
  }

  /**
   * Process the main query, and add it to the {@link org.apache.lucene.search.BooleanQuery} that will be executed.
   * Perform the expansion to multiple fields if necessary by creating a
   * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} clause for each field query.
   */
  private void processMainQuery(BooleanQuery query, final Map<String, Float> boosts, final String qstr)
  throws SyntaxError {
    BooleanQuery bq = new BooleanQuery(true); // combine the main query for each field in a nested boolean query
    for (final String field : boosts.keySet()) {
      final Map<String, Analyzer> datatypeConfig = this.getDatatypeConfig(field);
      final Query q = this.parse(field, qstr, datatypeConfig);
      if (boosts.get(field) != null) {
        q.setBoost(boosts.get(field));
      }
      bq.add(q, Occur.SHOULD);
    }
    query.add(bq, Occur.MUST); // add the nested boolean query to the main query with the MUST operator - See issue #60
  }

  /**
   * Process the nested queries and add them as a (MUST) clause of the {@link org.apache.lucene.search.BooleanQuery}
   * that will be executed.
   */
  private void processNestedQuery(final BooleanQuery main, final SolrParams solrParams)
  throws SyntaxError {
    if (solrParams.getParams("nested") != null) {
      for (final String nested : solrParams.getParams("nested")) {
        final QParser baseParser = this.subQuery(nested, null);
        main.add(baseParser.getQuery(), Occur.MUST);
      }
    }
  }

  protected abstract Query parse(final String field, final String qstr,
                                 final Map<String, Analyzer> datatypeConfig)
  throws SyntaxError;

  /**
   * Create a new QParser for parsing an embedded nested query.
   * <p>
   * Remove the nested parameters from the original request to avoid infinite
   * recursion.
   */
  @Override
  public QParser subQuery(final String q, final String defaultType)
  throws SyntaxError {
    final QParser nestedParser = super.subQuery(q, defaultType);
    final NamedList<Object> params = nestedParser.getParams().toNamedList();
    params.remove("nested");
    nestedParser.setParams(SolrParams.toSolrParams(params));
    return nestedParser;
  }

  /**
   * Retrieve the datatype query analyzers associated to this field
   */
  private Map<String, Analyzer> getDatatypeConfig(final String field) {
    final Map<String, Analyzer> datatypeConfig = new HashMap<String, Analyzer>();
    final ExtendedJsonField fieldType = (ExtendedJsonField) req.getSchema().getFieldType(field);
    final Map<String, Datatype> datatypes = fieldType.getDatatypes();

    for (final Entry<String, Datatype> e : datatypes.entrySet()) {

      if (e.getValue().getQueryAnalyzer() == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Configuration Error: No analyzer defined for type 'query' in " +
          "datatype " + e.getKey());
      }

      datatypeConfig.put(e.getKey(), e.getValue().getQueryAnalyzer());
    }

    return datatypeConfig;
  }

  protected Operator getDefaultOperator() {
    final String val = params.get(QueryParsing.OP);
    Operator defaultOp = Operator.AND; // default AND operator
    if (val != null) {
      defaultOp = "AND".equals(val) ? Operator.AND : Operator.OR;
    }
    return defaultOp;
  }

  /**
   * Uses {@link SolrPluginUtils#parseFieldBoosts(String)} with the 'qf'
   * parameter. Falls back to the 'df' parameter or
   * {@link org.apache.solr.schema.IndexSchema#getDefaultSearchFieldName()}.
   */
  public static Map<String, Float> parseQueryFields(final IndexSchema indexSchema, final SolrParams solrParams)
  throws SyntaxError {
    final Map<String, Float> queryFields = SolrPluginUtils.parseFieldBoosts(solrParams.getParams(SirenParams.QF));
    if (queryFields.isEmpty()) {
      final String df = QueryParsing.getDefaultField(indexSchema, solrParams.get(CommonParams.DF));
      if (df == null) {
        throw new SyntaxError("Neither "+SirenParams.QF+", "+CommonParams.DF +", nor the default search field are present.");
      }
      queryFields.put(df, 1.0f);
    }
    checkFieldTypes(indexSchema, queryFields);
    return queryFields;
  }

  /**
   * Check if all fields are of type {@link com.sindicetech.siren.solr.schema.ExtendedJsonField}.
   */
  private static void checkFieldTypes(final IndexSchema indexSchema, final Map<String, Float> queryFields) {
    for (final String fieldName : queryFields.keySet()) {
      final FieldType fieldType = indexSchema.getFieldType(fieldName);
      if (!(fieldType instanceof ExtendedJsonField)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "FieldType: " + fieldName + " (" + fieldType.getTypeName() + ") do not support SIREn's tree query");
      }
    }
  }

}
