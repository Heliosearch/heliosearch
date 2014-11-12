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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.search.node.TwigQuery;
import com.sindicetech.siren.search.node.TwigQuery.EmptyRootQuery;

/**
 * An {@link ArrayQueryNode} represents a JSON-like array of nodes. This array is
 * mapped to a {@link TwigQuery}, possibly with an {@link EmptyRootQuery}.
 * 
 * <p>
 * 
 * Each child of an {@link ArrayQueryNode} has the same root node, which is
 * defined by {@link TwigQueryNode}.
 */
public class ArrayQueryNode
extends QueryNodeImpl {

  /**
   *
   * @param values a list of {@link QueryNode}s, evaluated on the same level
   *        in the document tree.
   */
  public ArrayQueryNode(final List<QueryNode> values) {
    this.allocate();
    this.setLeaf(false);
    this.add(values);
  }

  public ArrayQueryNode(final QueryNode value) {
    this.allocate();
    this.setLeaf(false);
    this.add(value);
  }

  @Override
  public String toString() {
    String s = "<array>\n";
    for (final QueryNode v: this.getChildren()) {
      s += "<el>\n" + v + "\n</el>\n";
    }
    s += "</array>";
    return s;
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    if (this.getChildren() == null || this.getChildren().size() == 0)
      return "";

    final StringBuilder sb = new StringBuilder();
    if (this.getChildren().size() == 1) {
      sb.append(this.getChildren().get(0).toQueryString(escapeSyntaxParser));
    } else {
      sb.append("[ ");
      for (int i = 0; i < this.getChildren().size(); i++) {
        sb.append(this.getChildren().get(i).toQueryString(escapeSyntaxParser));
        if (i + 1 != this.getChildren().size()) {
          sb.append(", ");
        }
      }
      sb.append(" ]");
    }

    // in case is root or the parent is a group node avoid parenthesis
    if ((this.getParent() != null && this.getParent() instanceof GroupQueryNode) || this.isRoot()) {
      return sb.toString();
    } else {
      return "( " + sb.toString() + " )";
    }
  }

}
