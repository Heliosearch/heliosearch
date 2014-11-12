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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;

/**
 * This processor change the default operator if a unary operator +
 * ({@link Modifier#MOD_REQ}) is found.
 *
 * <p>
 *
 * This is needed to correctly implement the query logic with unary operators
 * (SRN-106).
 */
public class ChangeDefaultOperatorNodeProcessor
extends QueryNodeProcessorImpl {

  boolean hasUnaryReqOperator = false;

  public ChangeDefaultOperatorNodeProcessor() {}

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {

    if (node.getParent() == null) { // node is root, we processed the tree
      if (hasUnaryReqOperator) { // we found a req modifier in the tree

        final QueryConfigHandler conf = this.getQueryConfigHandler();
        if (!conf.has(ConfigurationKeys.DEFAULT_OPERATOR)) {
          throw new IllegalArgumentException(
              "ConfigurationKeys.DEFAULT_OPERATOR should be set on the QueryConfigHandler");
        }
        conf.set(ConfigurationKeys.DEFAULT_OPERATOR, Operator.OR);
      }
    }
    return node;

  }

  @Override
  protected QueryNode preProcessNode(final QueryNode node)
  throws QueryNodeException {

    if (node instanceof ModifierQueryNode) {
      final Modifier mod = ((ModifierQueryNode) node).getModifier();
      if (mod == Modifier.MOD_REQ) {
        this.hasUnaryReqOperator = true;
      }
    }

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
  throws QueryNodeException {

    return children;

  }


}
