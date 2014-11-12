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
package com.sindicetech.siren.qparser.tree.processors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import com.sindicetech.siren.qparser.tree.nodes.ChildQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.VariableQueryNode;

import java.util.List;

/**
 * Assign the {@link ModifierQueryNode.Modifier#MOD_NONE} modifier to a
 * {@link com.sindicetech.siren.qparser.tree.nodes.VariableQueryNode}. This is necessary because
 * otherwise the subScorer will be null and the NodeBooleanClause.isRequired() will be true in
 * TwigQuery, see line 349 in TwigQuery, and thus the evaluation will be wrong.
 */
public class VariableModifierQueryNodeProcessor extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode preProcessNode(final QueryNode node) throws QueryNodeException {
    return node;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {
    if (node instanceof ChildQueryNode) {
      ChildQueryNode childNode = (ChildQueryNode) node;
      if (childNode.getChild() instanceof VariableQueryNode) {
        return new ChildQueryNode(childNode.getChild(), ModifierQueryNode.Modifier.MOD_NONE);
      }
    }
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children) throws QueryNodeException {
    return children;
  }

}
