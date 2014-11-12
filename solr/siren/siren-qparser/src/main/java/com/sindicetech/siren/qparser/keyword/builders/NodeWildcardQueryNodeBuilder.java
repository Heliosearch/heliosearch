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
package com.sindicetech.siren.qparser.keyword.builders;

import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.WildcardQueryNodeBuilder;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import com.sindicetech.siren.qparser.keyword.processors.MultiNodeTermRewriteMethodProcessor;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeWildcardQuery;

/**
 * Builds a {@link NodeWildcardQuery} object from a {@link WildcardQueryNode}
 * object.
 *
 * <p>
 *
 * Code taken from {@link WildcardQueryNodeBuilder} and adapted to SIREn
 */
public class NodeWildcardQueryNodeBuilder implements KeywordQueryBuilder {

  public NodeWildcardQueryNodeBuilder() {
    // empty constructor
  }

  public NodeWildcardQuery build(final QueryNode queryNode) throws QueryNodeException {
    final WildcardQueryNode wildcardNode = (WildcardQueryNode) queryNode;

    final NodeWildcardQuery q = new NodeWildcardQuery(new Term(wildcardNode.getFieldAsString(),
                                                         wildcardNode.getTextAsString()));

    final MultiNodeTermQuery.RewriteMethod method = (MultiNodeTermQuery.RewriteMethod) queryNode.getTag(MultiNodeTermRewriteMethodProcessor.TAG_ID);
    if (method != null) {
      q.setRewriteMethod(method);
    }

    // assign the datatype. We must always have a datatype assigned.
    q.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));
    return q;
  }

}
