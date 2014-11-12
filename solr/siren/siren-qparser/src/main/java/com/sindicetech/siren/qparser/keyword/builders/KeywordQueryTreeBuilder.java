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
package com.sindicetech.siren.qparser.keyword.builders;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.*;
import org.apache.lucene.queryparser.flexible.standard.builders.DummyQueryNodeBuilder;
import org.apache.lucene.queryparser.flexible.standard.builders.MatchNoDocsQueryNodeBuilder;
import org.apache.lucene.queryparser.flexible.standard.nodes.*;
import org.apache.lucene.queryparser.flexible.standard.processors.StandardQueryNodeProcessorPipeline;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.nodes.*;
import com.sindicetech.siren.qparser.keyword.processors.KeywordQueryNodeProcessorPipeline;

/**
 * This query tree builder only defines the necessary map to build a
 * {@link Query} tree object.
 *
 * <p>
 *
 * It should be used to generate a {@link Query} tree
 * object from a query node tree processed by a
 * {@link KeywordQueryNodeProcessorPipeline}.
 *
 * <p>
 *
 * Copied from {@link StandardQueryNodeProcessorPipeline} for the SIREn use case.
 *
 * @see QueryTreeBuilder
 * @see KeywordQueryNodeProcessorPipeline
 */
public class KeywordQueryTreeBuilder extends QueryTreeBuilder implements KeywordQueryBuilder {

  public KeywordQueryTreeBuilder() {
    // Create Siren queries
    this.setBuilder(FuzzyQueryNode.class, new NodeFuzzyQueryNodeBuilder());
    this.setBuilder(WildcardQueryNode.class, new NodeWildcardQueryNodeBuilder());
    this.setBuilder(TokenizedPhraseQueryNode.class, new NodePhraseQueryNodeBuilder());
    this.setBuilder(PrefixWildcardQueryNode.class, new NodePrefixWildcardQueryNodeBuilder());
    this.setBuilder(SlopQueryNode.class, new SlopQueryNodeBuilder());
    this.setBuilder(MultiPhraseQueryNode.class, new MultiPhraseQueryNodeBuilder());
    this.setBuilder(FieldQueryNode.class, new FieldQueryNodeBuilder());
    this.setBuilder(NodeNumericRangeQueryNode.class, new NodeNumericRangeQueryNodeBuilder());
    this.setBuilder(TermRangeQueryNode.class, new NodeTermRangeQueryNodeBuilder());
    this.setBuilder(RegexpQueryNode.class, new NodeRegexpQueryNodeBuilder());
    this.setBuilder(TwigQueryNode.class, new TwigQueryNodeBuilder());
    this.setBuilder(ArrayQueryNode.class, new ArrayQueryNodeBuilder());
    this.setBuilder(WildcardNodeQueryNode.class, new DummyQueryNodeBuilder());
    this.setBuilder(NodeBooleanQueryNode.class, new NodeBooleanQueryNodeBuilder());
    this.setBuilder(SpanBooleanQueryNode.class, new SpanBooleanQueryNodeBuilder());

    this.setBuilder(TopLevelQueryNode.class, new TopLevelQueryNodeBuilder());
    // Create Lucene queries
    this.setBuilder(GroupQueryNode.class, new GroupQueryNodeBuilder());
    this.setBuilder(ModifierQueryNode.class, new ModifierQueryNodeBuilder());
    this.setBuilder(MatchAllDocsQueryNode.class, new MatchAllDocsQueryNodeBuilder());
    this.setBuilder(NumericQueryNode.class, new DummyQueryNodeBuilder());
    this.setBuilder(BooleanQueryNode.class, new BooleanQueryNodeBuilder());
    this.setBuilder(MatchNoDocsQueryNode.class, new MatchNoDocsQueryNodeBuilder());
    this.setBuilder(BoostQueryNode.class, new BoostQueryNodeBuilder());
  }

  @Override
  public Query build(final QueryNode queryNode) throws QueryNodeException {
    try {
      return (Query) super.build(queryNode);
    }
    catch (final ClassCastException e) {
      throw new Error("Unsupported query construct", e);
    }
  }

}
