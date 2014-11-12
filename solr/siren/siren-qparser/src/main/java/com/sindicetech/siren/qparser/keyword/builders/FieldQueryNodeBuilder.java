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

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeTermQuery;
import com.sindicetech.siren.search.spans.TermSpanQuery;

/**
 * Builds a {@link NodeTermQuery} or a {@link com.sindicetech.siren.search.spans.TermSpanQuery} object from a
 * {@link FieldQueryNode} object.
 * <p>
 * If the {@link org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode} has the tag
 * {@link com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor#QUERYTYPE_TAG} set to
 * {@link QueryTypeProcessor#SPAN_QUERYTYPE}, it builds a {@link com.sindicetech.siren.search.spans.TermSpanQuery}.
 */
public class FieldQueryNodeBuilder implements KeywordQueryBuilder {

  public FieldQueryNodeBuilder() {}

  public NodeQuery build(QueryNode queryNode) throws QueryNodeException {
    final FieldQueryNode fieldNode = (FieldQueryNode) queryNode;

    // if it is tagged as a span query
    if (fieldNode.getTag(QueryTypeProcessor.QUERYTYPE_TAG) == QueryTypeProcessor.SPAN_QUERYTYPE) {
      // create the term span query
      TermSpanQuery tsq = new TermSpanQuery(new Term(fieldNode.getFieldAsString(), fieldNode.getTextAsString()));
      // assign the datatype. We must always have a datatype assigned.
      tsq.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));
      return tsq;
    }
    else {
      // create the node term query
      NodeTermQuery ntq = new NodeTermQuery(new Term(fieldNode.getFieldAsString(), fieldNode.getTextAsString()));
      // assign the datatype. We must always have a datatype assigned.
      ntq.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));
      return ntq;
    }
  }

}
