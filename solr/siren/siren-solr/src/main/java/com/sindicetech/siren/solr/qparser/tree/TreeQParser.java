/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is not an open-source software. It is owned by Sindice Limited. SIREn
 * is licensed for evaluation purposes only under the terms and conditions of
 * the Sindice Limited Development License Agreement. Any form of modification
 * or reverse-engineering of SIREn is forbidden. SIREn is distributed without
 * any warranty.
 */
package com.sindicetech.siren.solr.qparser.tree;

import com.sindicetech.siren.solr.schema.ConciseJsonField;
import com.sindicetech.siren.solr.schema.ExtendedJsonField;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SyntaxError;

import com.sindicetech.siren.qparser.tree.ConciseTreeQueryParser;
import com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser;
import com.sindicetech.siren.solr.qparser.SirenQParser;

import java.util.Map;

/**
 * Implementation of {@link com.sindicetech.siren.solr.qparser.SirenQParser} for the
 * {@link com.sindicetech.siren.qparser.tree.ConciseTreeQueryParser}.
 *
 * <p>
 *
 * The {@link TreeQParser} is in charge of parsing a SIREn's Concise Tree query
 * request.
 */
public class TreeQParser extends SirenQParser {

  public TreeQParser(final String qstr, final SolrParams localParams,
                     final SolrParams params, final SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  protected Query parse(final String field, final String qstr,
                        final Map<String, Analyzer> datatypeConfig)
  throws SyntaxError {
    ExtendedTreeQueryParser parser;

    FieldType fieldType = req.getSchema().getField(field).getType();
    if (fieldType instanceof ConciseJsonField) {
      parser = new ConciseTreeQueryParser();
    } else if (fieldType instanceof ExtendedJsonField) {
      parser = new ExtendedTreeQueryParser();
    } else {
      throw new RuntimeException(String.format("Field %s is of type %s which is neither %s nor %s which are the only " +
          "supported.",
          field, fieldType.getClass().getName(), ConciseJsonField.class.getName(), ExtendedJsonField.class.getName()));
    }

    parser.setDefaultOperator(this.getDefaultOperator());
    parser.getKeywordQueryParser().setQNames(qnames);
    parser.getKeywordQueryParser().setDatatypeAnalyzers(datatypeConfig);
    parser.getKeywordQueryParser().setAllowLeadingWildcard(this.allowLeadingWildcard);

    try {
      return parser.parse(qstr, field);
    }
    catch (final QueryNodeException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

}
