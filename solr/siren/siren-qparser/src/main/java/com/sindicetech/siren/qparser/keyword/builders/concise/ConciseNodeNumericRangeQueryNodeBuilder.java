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
package com.sindicetech.siren.qparser.keyword.builders.concise;

import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

import com.sindicetech.siren.qparser.keyword.builders.NodeNumericRangeQueryNodeBuilder;
import com.sindicetech.siren.qparser.keyword.config.ConciseKeywordQueryConfigHandler;
import com.sindicetech.siren.search.node.NodeNumericRangeQuery;

/**
 * An extension of the {@link com.sindicetech.siren.qparser.keyword.builders.NodeNumericRangeQueryNodeBuilder} that
 * constructs a {@link ConciseNodeNumericRangeQuery}. The {@link ConciseNodeNumericRangeQuery} prepends the attribute
 * label to the encoded query term.
 */
public class ConciseNodeNumericRangeQueryNodeBuilder extends NodeNumericRangeQueryNodeBuilder {

  private final QueryConfigHandler conf;

  /**
   * Constructs a {@link ConciseNodeNumericRangeQueryNodeBuilder} object.
   */
  public ConciseNodeNumericRangeQueryNodeBuilder(final QueryConfigHandler queryConf) {
    this.conf = queryConf;
  }

  public NodeNumericRangeQuery<? extends Number> build(final QueryNode queryNode) throws QueryNodeException {
    return super.build(queryNode);
  }

  @Override
  protected NodeNumericRangeQuery newNodeNumericRangeQuery(final NumericType numberType, final String field,
                                                           final int precisionStep,
                                                           final Number lowerNumber, final Number upperNumber,
                                                           final boolean minInclusive, final boolean maxInclusive)
  throws QueryNodeException {
    if (conf.has(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE)) {
      final String attribute = conf.get(ConciseKeywordQueryConfigHandler.ConciseKeywordConfigurationKeys.ATTRIBUTE);

      switch (numberType) {
        case LONG:
          return ConciseNodeNumericRangeQuery.newLongRange(field, attribute, precisionStep,
              (Long) lowerNumber, (Long) upperNumber, minInclusive, maxInclusive);

        case INT:
          return ConciseNodeNumericRangeQuery.newIntRange(field, attribute, precisionStep,
              (Integer) lowerNumber, (Integer) upperNumber, minInclusive, maxInclusive);

        case FLOAT:
          return ConciseNodeNumericRangeQuery.newFloatRange(field, attribute, precisionStep,
              (Float) lowerNumber, (Float) upperNumber, minInclusive, maxInclusive);

        case DOUBLE:
          return ConciseNodeNumericRangeQuery.newDoubleRange(field, attribute, precisionStep,
              (Double) lowerNumber, (Double) upperNumber, minInclusive, maxInclusive);

        default:
          throw new QueryNodeException(new MessageImpl(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, numberType));
      }
    }
    else {
      return super.newNodeNumericRangeQuery(numberType, field, precisionStep, lowerNumber, upperNumber, minInclusive, maxInclusive);
    }
  }

}
