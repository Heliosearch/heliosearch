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

/**
 * This {@link QueryNode} defines the datatype of all its descendant nodes.
 *
 * @see com.sindicetech.siren.qparser.keyword.processors.DatatypeProcessor
 */
public class DatatypeQueryNode
extends QueryNodeImpl {

  /**
   * This tag is used to set the label of the datatype to be used
   * on that query node.
   */
  public static final String DATATYPE_TAGID = DatatypeQueryNode.class.getName();

  /** The datatype label */
  private String datatype;

  public DatatypeQueryNode(final QueryNode qn,
                           final String datatype) {
    this.allocate();
    this.setLeaf(false);
    this.add(qn);
    this.setDatatype(datatype);
  }

  /**
   * Set the datatype and the tag
   * {@link com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode#DATATYPE_TAGID}.
   *
   * @param datatype the datatype to set
   */
  public void setDatatype(String datatype) {
    this.datatype = datatype;
    this.setTag(DATATYPE_TAGID, datatype);
  }

  /**
   * @return the datatype
   */
  public String getDatatype() {
    return datatype;
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    String s = this.getDatatype() + "(";
    for (final QueryNode child: this.getChildren()) {
      s += child.toString() + " ";
    }
    return s + ")";
  }

  @Override
  public String toString() {
    String s = "<datatype name=\"" + datatype + "\">\n";
    for (final QueryNode child: this.getChildren()) {
      s += child.toString() + "\n";
    }
    return s + "</datatype>";
  }

  /**
   * Returns the typed {@link QueryNode}
   */
  public QueryNode getChild() {
    return this.getChildren().get(0);
  }

}
