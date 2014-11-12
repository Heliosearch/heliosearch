/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
<<<<<<< HEAD:siren-core/src/main/java/org/sindice/siren/analysis/NumericTokenizer.java
 * SIREn is not an open-source software. It is owned by Sindice Limited. SIREn
 * is licensed for evaluation purposes only under the terms and conditions of
 * the Sindice Limited Development License Agreement. Any form of modification
 * or reverse-engineering of SIREn is forbidden. SIREn is distributed without
 * any warranty.
=======
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
>>>>>>> develop:siren-core/src/main/java/com/sindicetech/siren/analysis/NumericTokenizer.java
 */
package com.sindicetech.siren.analysis;

import com.sindicetech.siren.analysis.NumericAnalyzer.NumericParser;
import com.sindicetech.siren.analysis.attributes.NodeNumericTermAttribute;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeFactory;
import java.io.IOException;
import java.io.Reader;

/**
 * This class provides a TokenStream for indexing numeric values that is used in
 * {@link NumericAnalyzer}.
 *
 * <p>
 *
 * This tokenizer expects to receive a string representation of a numeric value
 * as input. It parses the input using {@link NumericParser#parseAndConvert(Reader)},
 * and uses a {@link NodeNumericTermAttribute} to generate the numeric token.
 */
public class NumericTokenizer extends Tokenizer {

  private final NodeNumericTermAttribute numericAtt = this.addAttribute(NodeNumericTermAttribute.class);
  private final CharTermAttribute termAtt = this.addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = this.addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);

  private final NumericParser<?> parser;
  private boolean isInitialised = false;

  /**
   * Creates a token stream for numeric values with the specified
   * <code>precisionStep</code>.
   */
  public NumericTokenizer(final Reader input,
                          final NumericParser<? extends Number> parser,
                          final int precisionStep) {
    this(input, parser, precisionStep, AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
  }

  /**
   * Expert: Creates a token stream for numeric values with the specified
   * <code>precisionStep</code> using the given
   * {@link org.apache.lucene.util.AttributeFactory}.
   */
  public NumericTokenizer(final Reader input,
                          final NumericParser<? extends Number> parser,
                          final int precisionStep,
                          final AttributeFactory factory) {
    super(factory, input);
    this.parser = parser;
    numericAtt.setPrecisionStep(precisionStep);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    isInitialised = false;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    // initialise the numeric attribute
    if (!isInitialised) {
      final long value = parser.parseAndConvert(this.input);
      numericAtt.init(parser.getNumericType(), value, parser.getValueSize());
      isInitialised = true;
    }

    // this will only clear all other attributes in this TokenStream
    this.clearAttributes();

    // increment the shift and generate next token
    final boolean hasNext = numericAtt.incrementShift(termAtt);
    // set other attributes after the call to incrementShift since getShift
    // is undefined before first call
    typeAtt.setType((numericAtt.getShift() == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC);
    posIncrAtt.setPositionIncrement((numericAtt.getShift() == 0) ? 1 : 0);

    return hasNext;
  }

}
