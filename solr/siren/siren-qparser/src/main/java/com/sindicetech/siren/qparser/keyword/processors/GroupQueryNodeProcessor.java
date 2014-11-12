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
package com.sindicetech.siren.qparser.keyword.processors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.*;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.queryparser.flexible.standard.nodes.BooleanModifierNode;

import com.sindicetech.siren.qparser.keyword.nodes.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The {@link SyntaxParser} generates query node trees that consider the boolean
 * operator precedence, but Lucene current syntax does not support boolean
 * precedence, so this processor remove all the precedence and apply the
 * equivalent modifier according to the boolean operation defined on an specific
 * query node.
 *
 * <p>
 *
 * The original {@link org.apache.lucene.queryparser.flexible.standard.processors.GroupQueryNodeProcessor}
 * was not supporting correctly nested groups. This processor interprets a
 * {@link GroupQueryNode} and merges it with the node above it.
 */
// TODO: It is an ugly implementation that does not follow the tree traversal logic
public class GroupQueryNodeProcessor implements QueryNodeProcessor {

  private ArrayList<QueryNode> queryNodeList;

  private boolean latestNodeVerified;

  private QueryConfigHandler queryConfig;

  private Boolean usingAnd = false;

  public GroupQueryNodeProcessor() {
    // empty constructor
  }

  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    if (!this.getQueryConfigHandler().has(ConfigurationKeys.DEFAULT_OPERATOR)) {
      throw new IllegalArgumentException("DEFAULT_OPERATOR should be set on the QueryConfigHandler");
    }

    this.usingAnd = Operator.AND == this.getQueryConfigHandler().get(ConfigurationKeys.DEFAULT_OPERATOR) ? true : false;

    if (queryTree instanceof GroupQueryNode) {
      queryTree = ((GroupQueryNode) queryTree).getChild();
    }

    this.queryNodeList = new ArrayList<QueryNode>();
    this.latestNodeVerified = false;
    this.readTree(queryTree);

    if (queryTree instanceof BooleanQueryNode) {
      queryTree.set(this.queryNodeList);
      return queryTree;
    }
    else {
      return new BooleanQueryNode(this.queryNodeList);
    }

  }

  private void readTree(final QueryNode node) throws QueryNodeException {

    if (node instanceof BooleanQueryNode) {
      final List<QueryNode> children = node.getChildren();

      if (children != null && children.size() > 0) {

        for (int i = 0; i < children.size() - 1; i++) {
          this.readTree(children.get(i));
        }

        this.processNode(node);
        this.readTree(children.get(children.size() - 1));

      } else {
        this.processNode(node);
      }

    } else {
      this.processNode(node);
    }

  }

  private void processNode(final QueryNode node) throws QueryNodeException {
    if (node instanceof AndQueryNode || node instanceof OrQueryNode) {
      if (!this.latestNodeVerified && !this.queryNodeList.isEmpty()) {
        this.queryNodeList.add(this.applyModifier(this.queryNodeList
            .remove(this.queryNodeList.size() - 1), node));
        this.latestNodeVerified = true;
      }
    }
    else if (node instanceof GroupQueryNode) {
      final ArrayList<QueryNode> actualQueryNodeList = this.queryNodeList;
      actualQueryNodeList.add(this.applyModifier(this.process(node), node.getParent()));
      this.queryNodeList = actualQueryNodeList;
      this.latestNodeVerified = false;
    }
    else if (node instanceof TwigQueryNode) {
      final ArrayList<QueryNode> actualQueryNodeList = this.queryNodeList;
      final TwigQueryNode twigNode = (TwigQueryNode) node;
      final QueryNode root = twigNode.getRoot();
      final QueryNode child = twigNode.getChild();
      if (!(root instanceof WildcardNodeQueryNode)) { // the root is not empty
        twigNode.setRoot(this.process(root));
      }
      if (!(child instanceof WildcardNodeQueryNode)) { // the child is not empty
        twigNode.setChild(this.process(child));
      }
      actualQueryNodeList.add(twigNode);
      this.queryNodeList = actualQueryNodeList;
      this.latestNodeVerified = false;
    }
    else if (node instanceof ArrayQueryNode) {
      final ArrayList<QueryNode> actualQueryNodeList = this.queryNodeList;
      final ArrayQueryNode arrayNode = (ArrayQueryNode) node;
      final List<QueryNode> children = arrayNode.getChildren();
      final List<QueryNode> newChildren = new ArrayList<QueryNode>();
      for (final QueryNode child : children) {
        // The unary modifier sets the occurrence of this value in the TwigQuery
        if (!(child instanceof ModifierQueryNode)) {
          newChildren.add(this.process(child));
        } else {
          newChildren.add(child);
        }
      }
      arrayNode.set(newChildren);
      actualQueryNodeList.add(arrayNode);
      this.queryNodeList = actualQueryNodeList;
      this.latestNodeVerified = false;
    }
    else if (node instanceof TopLevelQueryNode) {
      final ArrayList<QueryNode> actualQueryNodeList = this.queryNodeList;
      final TopLevelQueryNode topNode = (TopLevelQueryNode) node;
      final QueryNode child = topNode.getChildren().get(0);
      topNode.set(Arrays.asList(this.process(child)));
      actualQueryNodeList.add(topNode);
      this.queryNodeList = actualQueryNodeList;
      this.latestNodeVerified = false;
    }
    else if (node instanceof DatatypeQueryNode) {
      final ArrayList<QueryNode> actualQueryNodeList = this.queryNodeList;
      final DatatypeQueryNode dtNode = (DatatypeQueryNode) node;
      final QueryNode child = dtNode.getChild();
      dtNode.set(Arrays.asList(this.applyModifier(this.process(child), node.getParent())));
      actualQueryNodeList.add(dtNode);
      this.queryNodeList = actualQueryNodeList;
      this.latestNodeVerified = false;
    }
    else if (!(node instanceof BooleanQueryNode)) {
      // if the parent node is a datatype query node, we have to skip it and return its parent
      QueryNode parent = node.getParent();
      parent = (parent instanceof DatatypeQueryNode) ? parent.getParent() : parent;
      // Apply the modifier and add it back to the list
      this.queryNodeList.add(this.applyModifier(node, parent));
      this.latestNodeVerified = false;
    }
  }

  /**
   */
  private QueryNode applyModifier(final QueryNode node, final QueryNode parent) {
    // if the default operator is AND
    if (this.usingAnd) {
      // if the parent is a or operator
      if (parent instanceof OrQueryNode) {
        // remove the modifier
        if (node instanceof ModifierQueryNode) {
          final ModifierQueryNode modNode = (ModifierQueryNode) node;
          if (modNode.getModifier() == Modifier.MOD_REQ) {
            return modNode.getChild();
          }
        }
        // at this stage, the node does not have modifier, which means it is an SHOULD clause
      }
      // in any other case, add the MOD_REQ modifier as default
      else {
        if (node instanceof ModifierQueryNode) {
          final ModifierQueryNode modNode = (ModifierQueryNode) node;
          if (modNode.getModifier() == Modifier.MOD_NONE) {
            return new BooleanModifierNode(modNode.getChild(), Modifier.MOD_REQ);
          }
        }
        else {
          return new BooleanModifierNode(node, Modifier.MOD_REQ);
        }
      }
    }
    // if the default operator is OR
    else {
      // if the parent operator is AND
      if (parent instanceof AndQueryNode) {
        // add the MOD_REQ modifier
        if (node instanceof ModifierQueryNode) {
          final ModifierQueryNode modNode = (ModifierQueryNode) node;
          if (modNode.getModifier() == Modifier.MOD_NONE) {
            return new BooleanModifierNode(modNode.getChild(), Modifier.MOD_REQ);
          }
        }
        else {
          return new BooleanModifierNode(node, Modifier.MOD_REQ);
        }
      }
    }

    return node;
  }

  public QueryConfigHandler getQueryConfigHandler() {
    return this.queryConfig;
  }

  public void setQueryConfigHandler(final QueryConfigHandler queryConfigHandler) {
    this.queryConfig = queryConfigHandler;
  }

}
