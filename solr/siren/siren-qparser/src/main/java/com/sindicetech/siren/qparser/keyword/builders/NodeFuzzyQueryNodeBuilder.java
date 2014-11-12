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
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.FuzzyQuery;

import com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor;
import com.sindicetech.siren.search.node.NodeFuzzyQuery;
import com.sindicetech.siren.search.node.NodeQuery;
import com.sindicetech.siren.search.spans.MultiTermSpanQuery;

/**
 * Builds a {@link NodeFuzzyQuery} or a {@link MultiTermSpanQuery<NodeFuzzyQuery>} object from a
 * {@link FuzzyQueryNode} object.
 * <p>
 * If the {@link FuzzyQueryNode} has the tag
 * {@link com.sindicetech.siren.qparser.keyword.processors.QueryTypeProcessor#QUERYTYPE_TAG} set to
 * {@link QueryTypeProcessor#SPAN_QUERYTYPE}, it wraps the {@link NodeFuzzyQuery} with a
 * {@link MultiTermSpanQuery<NodeFuzzyQuery>}.
 */
public class NodeFuzzyQueryNodeBuilder implements KeywordQueryBuilder {

  public NodeFuzzyQueryNodeBuilder() {
    // empty constructor
  }

  public NodeQuery build(QueryNode queryNode) throws QueryNodeException {
    FuzzyQueryNode fuzzyNode = (FuzzyQueryNode) queryNode;
    String text = fuzzyNode.getTextAsString();

    int numEdits = FuzzyQuery.floatToEdits(fuzzyNode.getSimilarity(), text.codePointCount(0, text.length()));

    NodeFuzzyQuery fuzzyQuery = new NodeFuzzyQuery(new Term(fuzzyNode.getFieldAsString(), fuzzyNode.getTextAsString()),
      numEdits, fuzzyNode.getPrefixLength());
    // assign the datatype. We must always have a datatype assigned.
    fuzzyQuery.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));

    // if it is tagged as a span query
    if (fuzzyNode.getTag(QueryTypeProcessor.QUERYTYPE_TAG) == QueryTypeProcessor.SPAN_QUERYTYPE) {
      return new MultiTermSpanQuery<>(fuzzyQuery);
    }
    else {
      return fuzzyQuery;
    }
  }

}
