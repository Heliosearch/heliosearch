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

import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.search.BooleanQuery;

import com.sindicetech.siren.qparser.keyword.processors.NodeBooleanQueryNodeProcessor;
import com.sindicetech.siren.search.node.NodeBooleanQuery;

import java.util.Map;

/**
 * A {@link NodeBooleanQueryNode} is used to represent a boolean
 * combination of terms inside a SIREn node.
 *
 * <p>
 *
 * This is done in {@link NodeBooleanQueryNodeProcessor}.
 * A {@link NodeBooleanQueryNode} is used to indicate that a
 * {@link NodeBooleanQuery} must be built, rather than a {@link BooleanQuery}.
 *
 * <p>
 *
 * Copied from {@link BooleanQueryNode} for the SIREn use case.
 */
public class NodeBooleanQueryNode extends QueryNodeImpl {

  /**
   * @param bq the {@link BooleanQueryNode} to convert
   */
  public NodeBooleanQueryNode(final BooleanQueryNode bq) {
    this.setLeaf(false);
    this.allocate();
    this.set(bq.getChildren());
    // copy tags
    for (Map.Entry<String, Object> entry : bq.getTagMap().entrySet()) {
      this.setTag(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public String toString() {
    if (this.getChildren() == null || this.getChildren().size() == 0)
      return "<nodeBoolean operation='default'/>";
    final StringBuilder sb = new StringBuilder();
    sb.append("<nodeBoolean operation='default'>");
    for (final QueryNode child : this.getChildren()) {
      sb.append("\n");
      sb.append(child.toString());
    }
    sb.append("\n</nodeBoolean>");
    return sb.toString();
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    if (this.getChildren() == null || this.getChildren().size() == 0)
      return "";

    final StringBuilder sb = new StringBuilder();
    String filler = "";
    for (final QueryNode child : this.getChildren()) {
      sb.append(filler).append(child.toQueryString(escapeSyntaxParser));
      filler = " ";
    }

    // in case is root or the parent is a group node avoid parenthesis
    if ((this.getParent() != null && this.getParent() instanceof GroupQueryNode)
        || this.isRoot())
      return sb.toString();
    else
      return "( " + sb.toString() + " )";
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    final NodeBooleanQueryNode clone = (NodeBooleanQueryNode) super.cloneTree();

    // nothing to do here

    return clone;
  }

}
