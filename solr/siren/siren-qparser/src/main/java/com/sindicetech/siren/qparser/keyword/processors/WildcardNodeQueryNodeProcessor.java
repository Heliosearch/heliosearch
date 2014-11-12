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
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

import com.sindicetech.siren.qparser.keyword.nodes.ArrayQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.WildcardNodeQueryNode;

/**
 * This processor transforms a {@link FieldQueryNode} equal to <code>"*"</code>
 * into a {@link WildcardNodeQueryNode}.
 * 
 * <p>
 * 
 * This processor throws a {@link QueryNodeException} if it finds a
 * {@link TwigQueryNode} with both root and child being an
 * {@link WildcardNodeQueryNode}.
 */
public class WildcardNodeQueryNodeProcessor
extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode preProcessNode(final QueryNode node)
  throws QueryNodeException {
    if ((node.getParent() instanceof TwigQueryNode ||
         node.getParent() instanceof ArrayQueryNode) &&
        this.isEmptyNode(node)) {
      return new WildcardNodeQueryNode();
    }
    return node;
  }

  /**
   * Return <code>true</code> if the {@link QueryNode} is a {@link FieldQueryNode},
   * which text is equal to "*".
   */
  private boolean isEmptyNode(final QueryNode q) {
    if (q instanceof FieldQueryNode) {
      final FieldQueryNode fq = (FieldQueryNode) q;
      final CharSequence text = fq.getText();
      if (text != null && text.length() == 1 && text.charAt(0) == '*') {
        return true;
      }
    }
    return false;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node)
  throws QueryNodeException {
    if (node instanceof TwigQueryNode) {
      final TwigQueryNode twig = (TwigQueryNode) node;
      if (twig.getChild() instanceof WildcardNodeQueryNode &&
          twig.getRoot() instanceof WildcardNodeQueryNode) {
        throw new QueryNodeException(new MessageImpl("Twig with both root and child empty is not allowed."));
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
