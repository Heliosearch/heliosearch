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
package com.sindicetech.siren.analysis.attributes;

import org.apache.lucene.analysis.NumericTokenStream.NumericTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import com.sindicetech.siren.analysis.NumericTokenizer;
import com.sindicetech.siren.search.node.NodeNumericRangeQuery;

/**
 * <b>Expert:</b> This class provides an {@link Attribute} for the
 * {@link NumericTokenizer} for indexing numeric values that can be used by {@link
 * NodeNumericRangeQuery}.
 * <p>
 * This attribute provides a stream of tokens which iterates over
 * the different precisions of a given numeric value.
 * <p>
 * The string representation of each precision is prefixed by:
 * <ul>
 * <li> the numeric type of the value;
 * <li> the precision step;
 * </ul>
 * This prefix is in fact encoding the numeric type and precision step inside
 * the dictionary. This prefix is necessary for two reasons:
 * <ul>
 * <li> it avoids overlapping value of different numeric type, and therefore
 * avoid getting false-positive;
 * <li> enables better clustering of the values of a particular numeric type
 * in the dictionary.
 * </ul>
 */
public interface NodeNumericTermAttribute extends Attribute {

  /**
   * Return the numeric type of the value
   */
  NumericType getNumericType();

  /**
   * Returns the current shift value
   * <p>
   * Undefined before first call to
   * {@link #incrementShift(CharTermAttribute, NumericType)}
   */
  int getShift();

  /**
   * Returns the value size in bits (32 for {@code float}, {@code int}; 64 for
   * {@code double}, {@code long})
   */
  int getValueSize();

  /**
   * Set the precision step
   */
  void setPrecisionStep(int precisionStep);

  /**
   * Returns the precision step
   */
  int getPrecisionStep();

  /**
   * Initialise this attribute
   */
  void init(NumericType numericType, long value, int valSize);

  /**
   * Reset the current shift value to 0
   */
  void resetShift();

  /**
   * Increment the shift and generate the next token.
   * <p>
   * The original Lucene's {@link NumericTermAttribute} implements
   * {@link TermToBytesRefAttribute}. There is a conflict problem with the
   * {@link CharTermAttribute} used in higher-level SIREn's analyzers, which also
   * implements {@link TermToBytesRefAttribute}.
   * The problem is that the {@link AttributeSource} is not able to choose
   * between the two when requested an attribute implementing
   * {@link TermToBytesRefAttribute}, e.g., in TermsHashPerField.
   * <p>
   * The current solution is to fill the {@link BytesRef} attribute of the
   * {@link CharTermAttribute} with the encoded numeric value.
   *
   * @return True if there are still tokens, false if we reach the end of the
   * stream.
   */
  boolean incrementShift(CharTermAttribute termAtt);

}
