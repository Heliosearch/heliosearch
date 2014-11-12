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

package com.sindicetech.siren.qparser.keyword.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.qparser.keyword.builders.TopLevelQueryNodeBuilder;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;

/**
 * This {@link QueryNode} represents the top level {@link QueryNode} of a keyword query.
 *
 * <p>
 *
 * It is used in {@link TopLevelQueryNodeBuilder} for wrapping
 * primitive queries, e.g., {@link NodeTermQuery}, into a
 * {@link LuceneProxyNodeQuery}.
 */
public class TopLevelQueryNode extends QueryNodeImpl {

  public TopLevelQueryNode(final QueryNode q) {
    this.allocate();
    this.setLeaf(false);
    this.add(q);
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    return this.getChildren().get(0).toQueryString(escapeSyntaxParser);
  }

  @Override
  public String toString() {
    return "<top>\n" + this.getChildren().get(0) + "\n</top>";
  }

}
