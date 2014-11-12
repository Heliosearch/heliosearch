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

package com.sindicetech.siren.analysis;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType.NumericType;

import com.sindicetech.siren.search.node.NodeNumericRangeQuery;

/**
 * Abstraction over the analyzer for numeric datatype.
 * <p>
 * At indexing time, this class provides a {@link NumericTokenizer} for indexing
 * numeric values that can be used by a {@link NodeNumericRangeQuery}. At query
 * time, this class acts as a container for parameters that are required in the
 * query parsers to process numeric range queries.
 * <p>
 * See {@link NodeNumericRangeQuery} for more information about numeric range
 * queries.
 */
public abstract class NumericAnalyzer extends Analyzer {

  protected final int precisionStep;

  public NumericAnalyzer(final int precisionStep) {
    this.precisionStep = precisionStep;
  }

  /**
   * Returns the precision step of this analyzer.
   */
  public int getPrecisionStep() {
    return precisionStep;
  }

  /**
   * Returns the {@link NumericParser} associated to this analyzer.
   * @param <T>
   */
  public abstract NumericParser<? extends Number> getNumericParser();

  public abstract class NumericParser<T extends Number> {

    /**
     * Reads a textual representation of a numeric using a
     * {@link Reader}, parses the encoded numeric value and convert the
     * numeric value to a sortable signed int or long (in the case of a float or
     * double).
     * <p>
     * This is used at index time, in {@link NumericTokenizer}.
     */
    public abstract long parseAndConvert(Reader input) throws IOException;

    /**
     * Reads a textual representation of a numeric using a
     * {@link Reader}, and parses the encoded numeric value.
     * <p>
     * This is used at query time, for creating a {@link NodeNumericRangeQuery}.
     */
    public abstract T parse(Reader input) throws IOException;

    /**
     * Returns the {@link NumericType} of this parser.
     */
    public abstract NumericType getNumericType();

    /**
     * Returns the size in bits of the numeric value.
     */
    public abstract int getValueSize();

  }

}
