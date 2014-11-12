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
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;

import com.sindicetech.siren.qparser.keyword.processors.MultiNodeTermRewriteMethodProcessor;
import com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeRegexpQuery;
import com.sindicetech.siren.search.spans.MultiTermSpanQuery;

/**
 * Builds a {@link com.sindicetech.siren.search.node.NodeRegexpQuery} or a
 * {@link MultiTermSpanQuery< com.sindicetech.siren.search.node.NodeRegexpQuery >} object from a
 * {@link RegexpQueryNode} object.
 * <p>
 * If the {@link RegexpQueryNode} has the tag
 * {@link com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor#QUERYTYPE_TAG} set to
 * {@link QueryTypeProcessor#SPAN_QUERYTYPE}, it wraps the {@link com.sindicetech.siren.search.node.NodeRegexpQuery} with a
 * {@link MultiTermSpanQuery< com.sindicetech.siren.search.node.NodeRegexpQuery >}.
 */
public class NodeRegexpQueryNodeBuilder implements StandardQueryBuilder {

  public NodeRegexpQueryNodeBuilder() {
    // empty constructor
  }

  public NodeQuery build(final QueryNode queryNode) throws QueryNodeException {
    final RegexpQueryNode regexpNode = (RegexpQueryNode) queryNode;

    final NodeRegexpQuery q = new NodeRegexpQuery(new Term(regexpNode.getFieldAsString(), regexpNode.textToBytesRef()));
    // assign the datatype. We must always have a datatype assigned.
    q.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));

    final MultiNodeTermQuery.RewriteMethod method =
      (MultiNodeTermQuery.RewriteMethod) queryNode.getTag(MultiNodeTermRewriteMethodProcessor.TAG_ID);

    if (method != null) {
      q.setRewriteMethod(method);
    }

    // if it is tagged as a span query
    if (regexpNode.getTag(QueryTypeProcessor.QUERYTYPE_TAG) == QueryTypeProcessor.SPAN_QUERYTYPE) {
      return new MultiTermSpanQuery<>(q);
    }
    else {
      return q;
    }
  }

}
