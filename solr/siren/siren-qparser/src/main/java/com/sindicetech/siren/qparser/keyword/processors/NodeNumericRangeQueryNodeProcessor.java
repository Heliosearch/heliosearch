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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.NumericRangeQueryNodeProcessor;

import com.sindicetech.siren.analysis.NumericAnalyzer;
import com.sindicetech.siren.analysis.NumericAnalyzer.NumericParser;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.NodeNumericQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.NodeNumericRangeQueryNode;
import com.sindicetech.siren.util.ReusableCharArrayReader;

import java.util.List;
import java.util.Map;

/**
 * This processor is used to convert {@link TermRangeQueryNode}s to
 * {@link NodeNumericRangeQueryNode}s.
 *
 * <p>
 *
 * It gets the numeric {@link Analyzer}
 * that was previously tagged with {@link DatatypeQueryNode#DATATYPE_TAGID}
 * in {@link DatatypeProcessor}. If it is set and is a {@link NumericAnalyzer},
 * it considers this {@link TermRangeQueryNode} to be a numeric range query and
 * converts it to a {@link NodeNumericRangeQueryNode}. It also assigns the {@link TermRangeQueryNode}'s datatype
 * to the newly created {@link NodeNumericRangeQueryNode}.
 *
 * <p>
 *
 * Class copied from {@link NumericRangeQueryNodeProcessor} and modified for the
 * SIREn use case.
 *
 * @see KeywordConfigurationKeys#DATATYPES_ANALYZERS
 * @see TermRangeQueryNode
 * @see NodeNumericRangeQueryNode
 */
public class NodeNumericRangeQueryNodeProcessor
extends QueryNodeProcessorImpl {

  /**
   * Constructs an empty {@link NodeNumericRangeQueryNodeProcessor} object.
   */
  public NodeNumericRangeQueryNodeProcessor() {
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node)
  throws QueryNodeException {
    if (node instanceof TermRangeQueryNode) {
      final TermRangeQueryNode termRangeNode = (TermRangeQueryNode) node;

      String datatype = (String) DatatypeProcessor.getDatatype(this.getQueryConfigHandler(), node);
      final Map<String, Analyzer> dts = this.getQueryConfigHandler().get(KeywordConfigurationKeys.DATATYPES_ANALYZERS);
      final Analyzer analyzer = dts.get(datatype);

      if (analyzer instanceof NumericAnalyzer) {
        final NumericAnalyzer na = (NumericAnalyzer) analyzer;

        final FieldQueryNode lower = termRangeNode.getLowerBound();
        final FieldQueryNode upper = termRangeNode.getUpperBound();

        final char[] lowerText = lower.getTextAsString().toCharArray();
        final char[] upperText = upper.getTextAsString().toCharArray();

        // Parse the lower and upper bound
        final NumericParser<?> parser = na.getNumericParser();
        final Number lowerNumber;
        try {
          if (lowerText.length == 0) { // open bound
            lowerNumber = null;
          } else {
            final ReusableCharArrayReader lowerReader = new ReusableCharArrayReader(lowerText);
            lowerNumber = parser.parse(lowerReader);
          }
        } catch (final Exception e) {
          throw new QueryNodeParseException(new MessageImpl(QueryParserMessages.COULD_NOT_PARSE_NUMBER,
            lowerText, parser.getNumericType() + " parser"), e);
        }
        final Number upperNumber;
        try {
          if (upperText.length == 0) { // open bound
            upperNumber = null;
          } else {
            final ReusableCharArrayReader upperReader = new ReusableCharArrayReader(upperText);
            upperNumber = parser.parse(upperReader);
          }
        } catch (final Exception e) {
          throw new QueryNodeParseException(new MessageImpl(QueryParserMessages.COULD_NOT_PARSE_NUMBER,
            upperText, parser.getNumericType() + " parser"), e);
        }

        // Create two NodeNumericQueryNode for the lower and upper bound
        final CharSequence field = termRangeNode.getField();
        final NodeNumericQueryNode lowerNode = new NodeNumericQueryNode(field, lowerNumber);
        // assign datatype
        lowerNode.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
        final NodeNumericQueryNode upperNode = new NodeNumericQueryNode(field, upperNumber);
        // assign datatype
        upperNode.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);

        final boolean lowerInclusive = termRangeNode.isLowerInclusive();
        final boolean upperInclusive = termRangeNode.isUpperInclusive();

        // Create the NodeNumericRangeQueryNode
        node = new NodeNumericRangeQueryNode(lowerNode, upperNode, lowerInclusive, upperInclusive, na);
        // assign datatype
        node.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
      }
    }
    return node;
  }

  @Override
  protected QueryNode preProcessNode(final QueryNode node)
  throws QueryNodeException {
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children)
  throws QueryNodeException {
    return children;
  }

}
