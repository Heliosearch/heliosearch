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

import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/**
 * A {@link com.sindicetech.siren.qparser.keyword.nodes.NodeGroupQueryNode} represents a node query introduced by
 * parenthesis in the query string.
 */
public class NodeGroupQueryNode extends GroupQueryNode {

  /**
   * This QueryNode is used to identify a span query on the original query string
   */
  public NodeGroupQueryNode(QueryNode query) {
    super(query);
  }

  @Override
  public String toString() {
    return "<node-group>" + "\n" + getChild().toString() + "\n</node-group>";
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (getChild() == null)
      return "";

    return "( " + getChild().toQueryString(escapeSyntaxParser) + " )";
  }

}