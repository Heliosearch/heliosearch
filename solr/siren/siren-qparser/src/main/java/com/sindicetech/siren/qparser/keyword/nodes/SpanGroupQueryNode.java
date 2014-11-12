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
 * A {@link SpanGroupQueryNode} represents a span query introduced by parenthesis in the query string.
 */
public class SpanGroupQueryNode extends GroupQueryNode {

  private final int slop;
  private final boolean inOrder;

  /**
   * This QueryNode is used to identify a span query on the original query string
   */
  public SpanGroupQueryNode(QueryNode query, Integer slop, boolean inOrder) {
    super(query);
    this.slop = slop;
    this.inOrder = inOrder;
  }

  public int getSlop() {
    return slop;
  }

  public boolean isInOrder() {
    return inOrder;
  }

  @Override
  public String toString() {
    return "<span-group>" + "\n" + getChild().toString() + "\n</span-group>";
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (getChild() == null)
      return "";

    return "< " + getChild().toQueryString(escapeSyntaxParser) + " >";
  }

}