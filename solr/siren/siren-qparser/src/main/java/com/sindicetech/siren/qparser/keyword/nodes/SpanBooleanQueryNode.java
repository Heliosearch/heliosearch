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
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.qparser.keyword.processors.SpanGroupQueryProcessor;

/**
 * A {@link com.sindicetech.siren.qparser.keyword.nodes.SpanBooleanQueryNode} is used to represent a boolean
 * combination of spans inside a SIREn node.
 *
 * <p>
 *
 * This is done in {@link com.sindicetech.siren.qparser.keyword.processors.NodeBooleanQueryNodeProcessor}.
 * A {@link com.sindicetech.siren.qparser.keyword.nodes.SpanBooleanQueryNode} is used to indicate that a
 * {@link com.sindicetech.siren.search.spans.BooleanSpanQuery} must be built, rather than a
 * {@link org.apache.lucene.search.BooleanQuery}.
 */
public class SpanBooleanQueryNode extends NodeBooleanQueryNode {

  private final int slop;
  private final boolean inOrder;

  /**
   * @param bq the {@link org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode} to convert
   */
  public SpanBooleanQueryNode(final BooleanQueryNode bq) {
    super(bq);
    this.slop = (Integer) bq.getTag(SpanGroupQueryProcessor.SLOP_TAG);
    this.inOrder = (Boolean) bq.getTag(SpanGroupQueryProcessor.INORDER_TAG);
  }

  public int getSlop() {
    return slop;
  }

  public boolean isInOrder() {
    return inOrder;
  }

  @Override
  public String toString() {
    if (this.getChildren() == null || this.getChildren().size() == 0)
      return "<spanBoolean operation='default'/>";
    final StringBuilder sb = new StringBuilder();
    sb.append("<spanBoolean operation='default'>");
    for (final QueryNode child : this.getChildren()) {
      sb.append("\n");
      sb.append(child.toString());
    }
    sb.append("\n</spanBoolean>");
    return sb.toString();
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    if (inOrder) {
      return super.toQueryString(escapeSyntaxParser) + "#" + slop;
    }
    else {
      return super.toQueryString(escapeSyntaxParser) + "~" + slop;
    }
  }

}
