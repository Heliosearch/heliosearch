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

import org.apache.lucene.queryparser.flexible.core.nodes.FieldValuePairQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.qparser.tree.parser.LevelPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.RangePropertyParser;

/**
 * This query node represents a node query that holds a node boolean expression
 * and a field.
 */
public class NodeQueryNode extends QueryNodeImpl
implements FieldValuePairQueryNode<CharSequence>, TextableQueryNode {

  /**
   * The node query's field
   */
  protected CharSequence field;

  /**
   * The node query's text.
   */
  protected CharSequence text;

  /**
   * The node query's attribute (concise model only)
   */
  protected CharSequence attribute;

  public NodeQueryNode() {
    this.setLeaf(true);
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CharSequence getField() {
    return this.field;
  }

  @Override
  public void setField(final CharSequence fieldName) {
    this.field = fieldName;
  }

  @Override
  public void setValue(final CharSequence value) {
    this.setText(value);
  }

  public void setAttribute(final CharSequence attribute) {
    this.attribute = attribute;
  }

  @Override
  public CharSequence getValue() {
    return this.getText();
  }

  @Override
  public CharSequence getText() {
    return this.text;
  }

  public CharSequence getAttribute() {
    return this.attribute;
  }

  @Override
  public void setText(final CharSequence text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return "<node field='" + this.field + "' text='" + this.text +
      "' level='" + this.getTag(LevelPropertyParser.LEVEL_PROPERTY) +
      "' range='" + this.getTag(RangePropertyParser.RANGE_PROPERTY) + "'/>";
  }

}
