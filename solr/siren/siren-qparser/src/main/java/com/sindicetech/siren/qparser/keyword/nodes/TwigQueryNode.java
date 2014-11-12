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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.TwigQuery;

/**
 * A {@link TwigQueryNode} represents a structured query, i.e., a {@link TwigQuery}.
 *
 * <p>
 *
 * It is composed of two nodes, a root and of a direct child.
 *
 * <p>
 *
 * A node with multiple children can be built by passing an
 * {@link ArrayQueryNode} to the {@link #setChild(QueryNode)}.
 *
 * <p>
 *
 * A complex twig can be built by chaining multiple {@link TwigQueryNode}.
 */
public class TwigQueryNode
extends QueryNodeImpl
implements FieldableNode {

  /** The root of the Twig Query */
  private int rootLevel = -1;
  /** Position in the list of the root of the twig */
  private static final int ROOT_POS  = 0;
  /** Position in the list of the child of the twig */
  private static final int CHILD_POS = 1;

  /**
   * Build a {@link TwigQueryNode} where the root can be either
   * an {@link WildcardNodeQueryNode} or a {@link NodeBooleanQueryNode}.
   * In addition to the root, the child node can be an {@link ArrayQueryNode}
   * or another {@link TwigQueryNode}.
   *
   * @param root the {@link QueryNode} as the root of the twig
   * @param child the {@link QueryNode} as the child of the twig
   */
  public TwigQueryNode(final QueryNode root, final QueryNode child) {
    this.allocate();
    this.setLeaf(false);
    this.add(root);
    this.add(child);
  }

  /**
   * Set the root of the twig
   */
  public void setRoot(final QueryNode root) {
    final ArrayList<QueryNode> newChildren = new ArrayList<QueryNode>();
    newChildren.add(root);
    newChildren.add(this.getChild());
    this.set(newChildren);
  }

  /**
   * Returns the root of the twig.
   */
  public QueryNode getRoot() {
    return this.getChildren().get(ROOT_POS);
  }

  /**
   * Set the child of the twig.
   */
  public void setChild(final QueryNode child) {
    final ArrayList<QueryNode> newChildren = new ArrayList<QueryNode>();
    newChildren.add(this.getRoot());
    newChildren.add(child);
    this.set(newChildren);
  }

  /**
   * Returns the child of the twig.
   */
  public QueryNode getChild() {
    return this.getChildren().get(CHILD_POS);
  }

  /**
   * Set the level of the root node in the document tree.
   * @see NodeQuery#setLevelConstraint(int)
   */
  public void setRootLevel(final int rootLevel) {
    this.rootLevel = rootLevel;
  }

  /**
   * Get the level of the root node in the document tree.
   * 
   * <p>
   * 
   * If no root level has been set, <code>-1</code> is returned.
   * @see NodeQuery#setLevelConstraint(int)
   */
  public int getRootLevel() {
    return rootLevel;
  }

  @Override
  public CharSequence getField() {
    return this.doGetField(this.getChildren());
  }

  private CharSequence doGetField(final List<QueryNode> children) {
    if (children != null) {
      for (final QueryNode child : children) {
        if (child instanceof FieldableNode) {
          return ((FieldableNode) child).getField();
        } else if (child instanceof TwigQueryNode) {
          return ((TwigQueryNode) child).getField();
        }
        final CharSequence field = this.doGetField(child.getChildren());
        if (field != null) {
          return field;
        }
      }
    }
    return null;
  }

  @Override
  public void setField(final CharSequence fieldName) {
    this.doSetField(this.getChildren(), fieldName);
  }

  private void doSetField(final List<QueryNode> children, final CharSequence fieldName) {
    if (children != null) {
      for (final QueryNode child : children) {
        this.doSetField(child.getChildren(), fieldName);
        if (child instanceof FieldableNode) {
          ((FieldableNode) child).setField(fieldName);
        } else if (child instanceof TwigQueryNode) {
          ((TwigQueryNode) child).setField(fieldName);
        }
      }
    }
  }

  @Override
  public String toString() {
    final QueryNode att = this.getRoot();
    final QueryNode child = this.getChild();
    final String s = "<twig root=\"" + this.getRootLevel() + "\">\n" +
                 "<root>\n" + (att instanceof WildcardNodeQueryNode ? "" : att + "\n") + "</root>\n" +
                 "<child>\n" + (child instanceof WildcardNodeQueryNode ? "" : child + "\n") + "</child>\n" +
               "</twig>";
    return s;
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    if (this.getChildren() == null || this.getChildren().size() == 0)
      return "";

    final StringBuilder sb = new StringBuilder();
    final QueryNode root = this.getRoot();
    final QueryNode child = this.getChild();
    if (root instanceof WildcardNodeQueryNode) {
      sb.append("* : ");
    } else {
      sb.append(root.toQueryString(escapeSyntaxParser)).append(" : ");
    }
    if (child instanceof WildcardNodeQueryNode) {
      sb.append("*");
    } else {
      sb.append(child.toQueryString(escapeSyntaxParser));
    }

    // in case is root or the parent is a group node avoid parenthesis
    if ((this.getParent() != null && this.getParent() instanceof GroupQueryNode) || this.isRoot()) {
      return sb.toString();
    } else {
      return "( " + sb.toString() + " )";
    }
  }

}
