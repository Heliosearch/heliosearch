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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

/**
 * Default implementation of the {@link NodeNumericTermAttribute}.
 */
public class NodeNumericTermAttributeImpl
extends AttributeImpl
implements NodeNumericTermAttribute {

  private NumericType numericType;
  private long     value    = 0L;
  private int      valueSize = 0, shift = 0, precisionStep = 0;

  /**
   * Char array containing the string representation of the numeric type
   */
  private char[] numericTypeCharArray;

  /**
   * Char array containing the string representation of the precision step
   */
  private char[] precisionStepCharArray;

  /**
   * Bytes buffer that will hold the current prefix coded bits of the value
   */
  private final BytesRefBuilder bytesRef  = new BytesRefBuilder();

  public NumericType getNumericType() { return numericType; }

  public int getShift() {
    // substract precistionStep to take into consideration the increment in
    // incrementShift
    return shift - precisionStep;
  }

  public void resetShift() { this.shift = 0; }

  public int getValueSize() { return valueSize; }

  public void setPrecisionStep(final int precisionStep) {
    if (precisionStep < 1) {
      throw new IllegalArgumentException("precisionStep must be >=1");
    }
    this.precisionStep = precisionStep;
    precisionStepCharArray = String.valueOf(precisionStep).toCharArray();
  }

  public int getPrecisionStep() { return precisionStep; }

  public void init(final NumericType numericType, final long value,
                   final int valueSize) {
    this.numericType = numericType;
    numericTypeCharArray = numericType.toString().toCharArray();
    this.value = value;
    this.valueSize = valueSize;
    this.shift = 0;
  }

  private void bytesRefToChar(final CharTermAttribute termAtt) {
    final char[] buffer;
    final int prefixSize = numericTypeCharArray.length + precisionStepCharArray.length;

    switch (valueSize) {
      case 64:
        NumericUtils.longToPrefixCoded(value, shift, bytesRef);
        buffer = termAtt.resizeBuffer(NumericUtils.BUF_SIZE_LONG + prefixSize);
        break;

      case 32:
        NumericUtils.intToPrefixCoded((int) value, shift, bytesRef);
        buffer = termAtt.resizeBuffer(NumericUtils.BUF_SIZE_INT + prefixSize);
        break;

      default:
        // should not happen
        throw new IllegalArgumentException("valueSize must be 32 or 64");
    }

    // Prepend the numericType
    System.arraycopy(numericTypeCharArray, 0, buffer, 0, numericTypeCharArray.length);
    // Prepend the precision step
    System.arraycopy(precisionStepCharArray, 0, buffer, numericTypeCharArray.length, precisionStepCharArray.length);
    // append the numeric encoded value
    BytesRef ref = bytesRef.get();
    for (int i = ref.offset; i < ref.length; i++) {
      buffer[prefixSize + i] = (char) ref.bytes[i];
    }
    termAtt.setLength(prefixSize + (ref.length - ref.offset));
  }

  public boolean incrementShift(final CharTermAttribute termAtt) {
    // check if we reach end of the stream
    if (shift >= valueSize) {
      return false;
    }

    try {
      // generate the next token and update the char term attribute
      this.bytesRefToChar(termAtt);
      // increment shift for next token
      shift += precisionStep;
      return true;
    }
    catch (final IllegalArgumentException iae) {
      // return empty token before first or after last
      termAtt.setEmpty();
      // ends the numeric tokenstream
      shift = valueSize;
      return false;
    }
  }

  @Override
  public void clear() {
    // this attribute has no contents to clear!
    // we keep it untouched as it's fully controlled by outer class.
  }

  @Override
  public void copyTo(final AttributeImpl target) {
    final NodeNumericTermAttribute a = (NodeNumericTermAttribute) target;
    a.setPrecisionStep(precisionStep);
    a.init(numericType, value, valueSize);
  }

}
