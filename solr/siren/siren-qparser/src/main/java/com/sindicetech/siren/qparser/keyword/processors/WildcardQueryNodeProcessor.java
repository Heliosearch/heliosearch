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
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import com.sindicetech.siren.qparser.keyword.nodes.ProtectedQueryNode;

/**
 * Wildcards within a {@link ProtectedQueryNode} are not processed
 *
 * <p>
 *
 * Copied from {@link org.apache.lucene.queryparser.flexible.standard.processors.WildcardQueryNodeProcessor}
 * and adapted for the SIREn use case.
 */
public class WildcardQueryNodeProcessor
extends org.apache.lucene.queryparser.flexible.standard.processors.WildcardQueryNodeProcessor {

  @Override
  protected QueryNode postProcessNode(final QueryNode node)
  throws QueryNodeException {
    if (node instanceof ProtectedQueryNode) {
      return node;
    }
    return super.postProcessNode(node);
  }

}
