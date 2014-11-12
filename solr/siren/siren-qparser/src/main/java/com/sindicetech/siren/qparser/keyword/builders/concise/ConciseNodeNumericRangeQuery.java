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
import org.apache.lucene.util.NumericUtils;

import com.sindicetech.siren.search.node.NodeNumericRangeQuery;

/**
 * An extension of the {@link NodeNumericRangeQuery} that will prepend an attribute label to the term prefix.
 */
public final class ConciseNodeNumericRangeQuery<T extends Number> extends NodeNumericRangeQuery<T> {

  protected ConciseNodeNumericRangeQuery(final String field,
                                         final String attribute,
                                         final int precisionStep,
                                         final NumericType dataType,
                                         final T min, final T max,
                                         final boolean minInclusive,
                                         final boolean maxInclusive) {
    super(field, precisionStep, dataType, min, max, minInclusive, maxInclusive);
    // Prepend the attribute to the term prefix
    this.termPrefix = ConciseNodeBuilderUtil.prepend(new StringBuilder(), attribute, this.termPrefix);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>long</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Long> newLongRange(final String field, final String attribute,
      final int precisionStep, final Long min, final Long max, final boolean minInclusive, final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Long>(field, attribute,
      precisionStep, NumericType.LONG, min, max, minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>long</code>
   * range using the default <code>precisionStep</code> {@link org.apache.lucene.util.NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Long> newLongRange(final String field, final String attribute,
      final Long min, final Long max, final boolean minInclusive, final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Long>(field, attribute,
      NumericUtils.PRECISION_STEP_DEFAULT, NumericType.LONG, min, max, minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>int</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Integer> newIntRange(final String field, final String attribute,
      final int precisionStep, final Integer min, final Integer max,
      final boolean minInclusive, final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Integer>(field, attribute,
      precisionStep, NumericType.INT, min, max, minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>int</code>
   * range using the default <code>precisionStep</code> {@link org.apache.lucene.util.NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Integer> newIntRange(final String field, final String attribute,
      final Integer min, final Integer max, final boolean minInclusive, final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Integer>(field, attribute,
      NumericUtils.PRECISION_STEP_DEFAULT, NumericType.INT, min, max, minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>double</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Double> newDoubleRange(final String field, final String attribute,
      final int precisionStep, final Double min, final Double max,
      final boolean minInclusive, final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Double>(field, attribute, precisionStep,
      NumericType.DOUBLE, min, max, minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>double</code>
   * range using the default <code>precisionStep</code> {@link org.apache.lucene.util.NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Double> newDoubleRange(final String field, final String attribute,
      final Double min, final Double max, final boolean minInclusive,
      final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Double>(field, attribute,
    NumericUtils.PRECISION_STEP_DEFAULT, NumericType.DOUBLE, min, max,
      minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>float</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Float> newFloatRange(final String field, final String attribute,
      final int precisionStep, final Float min, final Float max,
      final boolean minInclusive, final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Float>(field, attribute, precisionStep,
      NumericType.FLOAT, min, max, minInclusive, maxInclusive);
  }

  /**
   * Factory that creates a {@link ConciseNodeNumericRangeQuery}, that queries a <code>float</code>
   * range using the default <code>precisionStep</code> {@link org.apache.lucene.util.NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static ConciseNodeNumericRangeQuery<Float> newFloatRange(final String field, final String attribute,
    final Float min, final Float max, final boolean minInclusive,
    final boolean maxInclusive) {
    return new ConciseNodeNumericRangeQuery<Float>(field, attribute,
    NumericUtils.PRECISION_STEP_DEFAULT, NumericType.FLOAT, min, max,
      minInclusive, maxInclusive);
  }

}

