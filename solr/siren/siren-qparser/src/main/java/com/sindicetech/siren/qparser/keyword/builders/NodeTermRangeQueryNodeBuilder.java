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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

import com.sindicetech.siren.qparser.keyword.processors.MultiNodeTermRewriteMethodProcessor;
import com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor;
import com.sindicetech.siren.search.node.MultiNodeTermQuery;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.node.NodeTermRangeQuery;
import com.sindicetech.siren.search.spans.MultiTermSpanQuery;

/**
 * Builds a {@link com.sindicetech.siren.search.node.NodeTermRangeQuery} or a
 * {@link MultiTermSpanQuery< com.sindicetech.siren.search.node.NodeTermRangeQuery >} object from a
 * {@link TermRangeQueryNode} object.
 * <p>
 * If the {@link TermRangeQueryNode} has the tag
 * {@link com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor#QUERYTYPE_TAG} set to
 * {@link QueryTypeProcessor#SPAN_QUERYTYPE}, it wraps the {@link com.sindicetech.siren.search.node.NodeTermRangeQuery} with a
 * {@link MultiTermSpanQuery< com.sindicetech.siren.search.node.NodeTermRangeQuery >}.
 */
public class NodeTermRangeQueryNodeBuilder
implements KeywordQueryBuilder {

  public NodeTermRangeQueryNodeBuilder() {
  }

  public NodeQuery build(final QueryNode queryNode) throws QueryNodeException {
    final TermRangeQueryNode rangeNode = (TermRangeQueryNode) queryNode;
    final FieldQueryNode upper = rangeNode.getUpperBound();
    final FieldQueryNode lower = rangeNode.getLowerBound();

    final String field = StringUtils.toString(rangeNode.getField());
    String lowerText = lower.getTextAsString();
    String upperText = upper.getTextAsString();

    if (lowerText.length() == 0) {
      lowerText = null;
    }

    if (upperText.length() == 0) {
      upperText = null;
    }

    final NodeTermRangeQuery rangeQuery = NodeTermRangeQuery.newStringRange(field, lowerText, upperText,
      rangeNode.isLowerInclusive(), rangeNode.isUpperInclusive());

    final MultiNodeTermQuery.RewriteMethod method =
      (MultiNodeTermQuery.RewriteMethod) queryNode.getTag(MultiNodeTermRewriteMethodProcessor.TAG_ID);

    if (method != null) {
      rangeQuery.setRewriteMethod(method);
    }

    // if it is tagged as a span query
    if (rangeNode.getTag(QueryTypeProcessor.QUERYTYPE_TAG) == QueryTypeProcessor.SPAN_QUERYTYPE) {
      return new MultiTermSpanQuery<>(rangeQuery);
    }
    else {
      return rangeQuery;
    }
  }

}
