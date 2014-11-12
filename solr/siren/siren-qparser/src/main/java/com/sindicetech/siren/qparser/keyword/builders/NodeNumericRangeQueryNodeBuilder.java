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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.builders.NumericRangeQueryNodeBuilder;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericQueryNode;

import com.sindicetech.siren.analysis.NumericAnalyzer;
import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.NodeNumericRangeQueryNode;
import com.sindicetech.siren.search.node.NodeNumericRangeQuery;

/**
 * Builds a {@link NodeNumericRangeQuery} object from a
 * {@link NodeNumericRangeQueryNode} object.
 *
 * <p>
 *
 * Class copied from {@link NumericRangeQueryNodeBuilder} for the Siren use case:
 * in Siren, we use an {@link Analyzer} instead of a {@link NumericConfig}
 * configuration object.
 */
public class NodeNumericRangeQueryNodeBuilder implements KeywordQueryBuilder {

  /**
   * Constructs a {@link NodeNumericRangeQueryNodeBuilder} object.
   */
  public NodeNumericRangeQueryNodeBuilder() {}

  public NodeNumericRangeQuery<? extends Number> build(final QueryNode queryNode) throws QueryNodeException {
    final NodeNumericRangeQueryNode numericRangeNode = (NodeNumericRangeQueryNode) queryNode;

    final NumericQueryNode lowerNumericNode = numericRangeNode.getLowerBound();
    final NumericQueryNode upperNumericNode = numericRangeNode.getUpperBound();

    final Number lowerNumber, upperNumber;

    if (lowerNumericNode != null) {
      lowerNumber = lowerNumericNode.getValue();
    }
    else {
      lowerNumber = null;
    }

    if (upperNumericNode != null) {
      upperNumber = upperNumericNode.getValue();
    }
    else {
      upperNumber = null;
    }

    final NumericAnalyzer numericAnalyzer = numericRangeNode.getNumericAnalyzer();
    final NumericType numberType = numericRangeNode.getNumericType();
    final String field = numericRangeNode.getField().toString();
    final boolean minInclusive = numericRangeNode.isLowerInclusive();
    final boolean maxInclusive = numericRangeNode.isUpperInclusive();
    final int precisionStep = numericAnalyzer.getPrecisionStep();

    NodeNumericRangeQuery query = this.newNodeNumericRangeQuery(numberType, field, precisionStep, lowerNumber,
      upperNumber, minInclusive, maxInclusive);
    // assign the datatype. We must always have a datatype assigned.
    query.setDatatype((String) queryNode.getTag(DatatypeQueryNode.DATATYPE_TAGID));
    return query;
  }

  protected NodeNumericRangeQuery newNodeNumericRangeQuery(final NumericType numberType, final String field,
                                                           final int precisionStep,
                                                           final Number lowerNumber, final Number upperNumber,
                                                           final boolean minInclusive, final boolean maxInclusive)
  throws QueryNodeException {
    switch (numberType) {
      case LONG:
        return NodeNumericRangeQuery.newLongRange(field, precisionStep,
          (Long) lowerNumber, (Long) upperNumber, minInclusive, maxInclusive);

      case INT:
        return NodeNumericRangeQuery.newIntRange(field, precisionStep,
          (Integer) lowerNumber, (Integer) upperNumber, minInclusive, maxInclusive);

      case FLOAT:
        return NodeNumericRangeQuery.newFloatRange(field, precisionStep,
          (Float) lowerNumber, (Float) upperNumber, minInclusive, maxInclusive);

      case DOUBLE:
        return NodeNumericRangeQuery.newDoubleRange(field, precisionStep,
          (Double) lowerNumber, (Double) upperNumber, minInclusive, maxInclusive);

      default:
        throw new QueryNodeException(new MessageImpl(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, numberType));
    }
  }

}
