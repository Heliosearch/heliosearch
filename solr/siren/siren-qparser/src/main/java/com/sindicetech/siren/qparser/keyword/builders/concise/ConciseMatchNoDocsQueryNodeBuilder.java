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
package com.sindicetech.siren.qparser.keyword.builders.concise;

import com.sindicetech.siren.qparser.keyword.builders.KeywordQueryBuilder;
import com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.keyword.processors.DatatypeProcessor;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.Query;

/**
 * A {@link org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode} is generated when the query string
 * is emtpy. In the concise model, an empty query string associated to an attribute indicates an attribute query.
 * Therefore, in the presence of an attribute, this builder creates a {@link com.sindicetech.siren.search.node.NodeTermQuery}
 * with the encoded attribute term. Otherwise, it returns an emtpy {@link NodeBooleanQuery}.
 */
public class ConciseMatchNoDocsQueryNodeBuilder implements KeywordQueryBuilder {

  private final QueryConfigHandler conf;

  private final StringBuilder builder = new StringBuilder();

  public ConciseMatchNoDocsQueryNodeBuilder(final QueryConfigHandler queryConf) {
    this.conf = queryConf;
  }

  @Override
  public Query build(final QueryNode queryNode) throws QueryNodeException {
    if (conf.has(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE)) {
      final String attribute = conf.get(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE);
      final String field = conf.get(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.FIELD);

      // create the node term query
      NodeTermQuery ntq = new NodeTermQuery(new Term(field, ConciseNodeBuilderUtil.prepend(builder, attribute, "")));

      // assign the datatype. We must always have a datatype assigned.
      String datatype = DatatypeProcessor.getDefaultDatatype(this.conf);
      ntq.setDatatype(datatype);

      return ntq;
    }
    else {
      return new NodeBooleanQuery();
    }
  }

}
