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
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.NumericQueryNodeProcessor;

import com.sindicetech.siren.analysis.NumericAnalyzer;
import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.NodeNumericQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.NodeNumericRangeQueryNode;
import com.sindicetech.siren.util.ReusableCharArrayReader;

import java.util.List;
import java.util.Map;

/**
 * This processor is used to convert {@link FieldQueryNode}s to
 * {@link NodeNumericRangeQueryNode}s.
 *
 * <p>
 *
 * It gets the numeric {@link Analyzer} that was previously tagged with
 * {@link DatatypeQueryNode#DATATYPE_TAGID}
 * in {@link DatatypeProcessor}. If set and is a {@link NumericAnalyzer},
 * it considers this {@link FieldQueryNode} to be a numeric query and converts
 * it to {@link NodeNumericRangeQueryNode} with upper and lower inclusive and
 * lower and upper equals to the value represented by the {@link FieldQueryNode}
 * converted to {@link Number}. It means that <b>1^^&lt;int&gt;</b> is converted
 * to <b>[1 TO 1]^^&lt;int&gt;</b>.
 *
 * <p>
 *
 * The datatype of the value does not depend on a field as in Lucene (see {@link DatatypeProcessor}).
 *
 * <p>
 *
 * Note that {@link FieldQueryNode}s children of a
 * {@link RangeQueryNode} are ignored.
 *
 * <p>
 *
 * Copied from {@link NumericQueryNodeProcessor} and modified for the
 * SIREn use case.
 *
 * @see KeywordConfigurationKeys#DATATYPES_ANALYZERS
 * @see FieldQueryNode
 * @see NumericQueryNode
 * @see NodeNumericRangeQueryNode
 */
public class NodeNumericQueryNodeProcessor
extends QueryNodeProcessorImpl {

  /**
   * Constructs an empty {@link NodeNumericQueryNodeProcessor} object.
   */
  public NodeNumericQueryNodeProcessor() {
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node)
  throws QueryNodeException {

    if (node instanceof FieldQueryNode && !(node.getParent() instanceof RangeQueryNode)) {
      final FieldQueryNode fieldNode = (FieldQueryNode) node;

      String datatype = (String) DatatypeProcessor.getDatatype(this.getQueryConfigHandler(), node);
      final Map<String, Analyzer> dts = this.getQueryConfigHandler().get(KeywordConfigurationKeys.DATATYPES_ANALYZERS);
      final Analyzer analyzer = dts.get(node.getTag(DatatypeQueryNode.DATATYPE_TAGID));

      if (analyzer instanceof NumericAnalyzer) {
        final NumericAnalyzer na = (NumericAnalyzer) analyzer;
        final char[] text = fieldNode.getTextAsString().toCharArray();
        final ReusableCharArrayReader textReader = new ReusableCharArrayReader(text);
        final Number number;
        try {
          number = na.getNumericParser().parse(textReader);
        } catch (final Exception e) {
          throw new QueryNodeParseException(new MessageImpl(QueryParserMessages.COULD_NOT_PARSE_NUMBER, text), e);
        }

        final CharSequence field = fieldNode.getField();
        final NodeNumericQueryNode lowerNode = new NodeNumericQueryNode(field, number);
        // assign datatype
        lowerNode.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
        final NodeNumericQueryNode upperNode = new NodeNumericQueryNode(field, number);
        // assign datatype
        upperNode.setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);

        // Create the NodeNumericRangeQueryNode
        node = new NodeNumericRangeQueryNode(lowerNode, upperNode, true, true, na);
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
