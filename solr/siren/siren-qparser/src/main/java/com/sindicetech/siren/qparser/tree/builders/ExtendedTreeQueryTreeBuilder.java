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
package com.sindicetech.siren.qparser.tree.builders;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.ModifierQueryNodeBuilder;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.nodes.*;
import com.sindicetech.siren.qparser.tree.processors.JsonQueryNodeProcessorPipeline;

/**
 * This query tree builder defines the necessary map to build a
 * {@link Query} tree object.
 *
 * <p>
 *
 * It should be used to generate a {@link Query} tree
 * object from a query node tree processed by a
 * {@link JsonQueryNodeProcessorPipeline}.
 *
 * @see QueryTreeBuilder
 * @see JsonQueryNodeProcessorPipeline
 */
public class ExtendedTreeQueryTreeBuilder extends QueryTreeBuilder
implements ExtendedTreeQueryBuilder {

  public ExtendedTreeQueryTreeBuilder(final ExtendedKeywordQueryParser keywordParser) {
    this.setBuilders(keywordParser);
  }

  public void setBuilders(final ExtendedKeywordQueryParser keywordParser) {
    this.setBuilder(TopLevelQueryNode.class, new TopLevelQueryNodeBuilder());
    this.setBuilder(NodeQueryNode.class, new NodeQueryNodeBuilder(keywordParser));
    this.setBuilder(ModifierQueryNode.class, new ModifierQueryNodeBuilder());
    this.setBuilder(DescendantQueryNode.class, new ModifierQueryNodeBuilder());
    this.setBuilder(ChildQueryNode.class, new ModifierQueryNodeBuilder());
    this.setBuilder(TwigQueryNode.class, new TwigQueryNodeBuilder(keywordParser));
    this.setBuilder(VariableQueryNode.class, new VariableQueryNodeBuilder());
    this.setBuilder(BooleanQueryNode.class, new BooleanQueryNodeBuilder());
  }

  @Override
  public Query build(final QueryNode queryNode) throws QueryNodeException {
    return (Query) super.build(queryNode);
  }

}
