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
package com.sindicetech.siren.qparser.tree.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.qparser.tree.parser.LevelPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.RangePropertyParser;

/**
 * This query node represents a twig query that holds a root query's node
 * boolean expression and a list of elements which can be either a
 * {@link ChildQueryNode} or a {@link DescendantQueryNode}.
 */
public class TwigQueryNode extends QueryNodeImpl implements FieldableNode {

  /**
   * The twig query's field
   */
  protected CharSequence field;

  /**
   * The root query's node boolean expression.
   */
  protected CharSequence root;

  public TwigQueryNode() {
    this.allocate();
    this.setLeaf(false);
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    throw new UnsupportedOperationException();
  }

  /**
   * Retrieves the root query's node boolean expression.
   */
  public CharSequence getRoot() {
    return root;
  }

  /**
   * Sets the root query's node boolean expression.
   */
  public void setRoot(final CharSequence text) {
    this.root = text;
  }

  /**
   * Return true if this twig query node has a root query
   */
  public boolean hasRoot() {
    return root != null;
  }

  @Override
  public CharSequence getField() {
    return field;
  }

  @Override
  public void setField(final CharSequence fieldName) {
    this.field = fieldName;
  }

  @Override
  public String toString() {
    if (this.getRoot() == null && this.getChildren().size() == 0) {
      return "<twig/>";
    }

    final StringBuilder sb = new StringBuilder();
    sb.append("<twig field='" + this.field + "' root='" + this.root +
      "' level='" + this.getTag(LevelPropertyParser.LEVEL_PROPERTY) +
      "' range='" + this.getTag(RangePropertyParser.RANGE_PROPERTY) + "'>");
    for (final QueryNode child : this.getChildren()) {
      sb.append("\n");
      sb.append(child.toString());
    }
    sb.append("\n</twig>");
    return sb.toString();
  }

}
