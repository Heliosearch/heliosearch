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

import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import com.sindicetech.siren.qparser.keyword.builders.KeywordQueryBuilder;
import com.sindicetech.siren.qparser.keyword.builders.KeywordQueryTreeBuilder;
import com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler;
import com.sindicetech.siren.qparser.keyword.nodes.NodeNumericRangeQueryNode;

/**
 * This query tree builder extends the {@link KeywordQueryTreeBuilder} to overwrite the original
 * {@link KeywordQueryBuilder} classes with their extended version for the concise model.
 */
public class ConciseKeywordQueryTreeBuilder extends KeywordQueryTreeBuilder {

  private final QueryConfigHandler queryConfig;

  public ConciseKeywordQueryTreeBuilder(final QueryConfigHandler queryConfig) {
    this.queryConfig = queryConfig;

    this.setBuilder(MatchNoDocsQueryNode.class, new ConciseMatchNoDocsQueryNodeBuilder(queryConfig));
    this.setBuilder(FuzzyQueryNode.class, new ConciseNodeFuzzyQueryNodeBuilder(queryConfig));
    this.setBuilder(WildcardQueryNode.class, new ConciseNodeWildcardQueryNodeBuilder(queryConfig));
    this.setBuilder(TokenizedPhraseQueryNode.class, new ConciseNodePhraseQueryNodeBuilder(queryConfig));
    this.setBuilder(PrefixWildcardQueryNode.class, new ConciseNodePrefixWildcardQueryNodeBuilder(queryConfig));
    this.setBuilder(FieldQueryNode.class, new ConciseFieldQueryNodeBuilder(queryConfig));
    this.setBuilder(NodeNumericRangeQueryNode.class, new ConciseNodeNumericRangeQueryNodeBuilder(queryConfig));
    this.setBuilder(RegexpQueryNode.class, new ConciseNodeRegexpQueryNodeBuilder(queryConfig));
  }

  public void setDefaultField(String defaultField) {
    this.queryConfig.set(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.FIELD, defaultField);
  }

}
