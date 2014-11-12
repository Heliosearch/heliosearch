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
package com.sindicetech.siren.qparser.keyword.nodes;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;
import org.apache.lucene.queryparser.flexible.standard.nodes.AbstractRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.NumericRangeQueryNode;

import com.sindicetech.siren.analysis.NumericAnalyzer;

/**
 * This {@link QueryNode} represents a range query composed by
 * {@link NodeNumericQueryNode} bounds, which means the bound values are
 * {@link Number}s.
 *
 * <p>
 *
 * The configuration in Siren of a numeric value is done through {@link Analyzer}s,
 * not through {@link NumericConfig}.
 *
 * <p>
 *
 * Class copied from {@link NumericRangeQueryNode} and adapted for the SIREn use
 * case.
 */
public class NodeNumericRangeQueryNode extends AbstractRangeQueryNode<NumericQueryNode> {

  public final NumericAnalyzer numericAnalyzer;
  public final NumericType numericType;

  /**
   * Constructs a {@link NodeNumericRangeQueryNode} object using the given
   * {@link NumericQueryNode} as its bounds and a {@link NumericAnalyzer}.
   *
   * @param lower the lower bound
   * @param upper the upper bound
   * @param lowerInclusive <code>true</code> if the lower bound is inclusive, otherwise, <code>false</code>
   * @param upperInclusive <code>true</code> if the upper bound is inclusive, otherwise, <code>false</code>
   * @param numericAnalyzer the {@link NumericAnalyzer} associated with the upper and lower bounds
   */
  public NodeNumericRangeQueryNode(final NumericQueryNode lower,
                                   final NumericQueryNode upper,
                                   final boolean lowerInclusive,
                                   final boolean upperInclusive,
                                   final NumericAnalyzer numericAnalyzer)
  throws QueryNodeException {
    if (numericAnalyzer == null) {
      throw new IllegalArgumentException("numericAnalyzer cannot be null!");
    }
    super.setBounds(lower, upper, lowerInclusive, upperInclusive);
    this.numericAnalyzer = numericAnalyzer;
    this.numericType = numericAnalyzer.getNumericParser().getNumericType();
  }

  /**
   * Returns the {@link NumericAnalyzer} associated with the lower and upper bounds.
   */
  public NumericAnalyzer getNumericAnalyzer() {
    return this.numericAnalyzer;
  }

  /**
   * Returns the {@link NumericType} of the lower and upper bounds.
   */
  public NumericType getNumericType() {
    return numericType;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("<numericRange lowerInclusive='");

    sb.append(this.isLowerInclusive()).append("' upperInclusive='").append(
        this.isUpperInclusive()).append(
        "' precisionStep='" + numericAnalyzer.getPrecisionStep()).append(
        "' type='" + numericType).append("'>\n");

    sb.append(this.getLowerBound()).append('\n');
    sb.append(this.getUpperBound()).append('\n');
    sb.append("</numericRange>");

    return sb.toString();
  }

}
