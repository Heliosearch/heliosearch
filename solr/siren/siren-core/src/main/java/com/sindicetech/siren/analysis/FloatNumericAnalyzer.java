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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.util.NumericUtils;

import com.sindicetech.siren.util.XSDPrimitiveTypeParser;

/**
 * An implementation of the {@link NumericAnalyzer} for float value.
 */
public class FloatNumericAnalyzer extends NumericAnalyzer {

  public FloatNumericAnalyzer(final int precisionStep) {
    super(precisionStep);
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName,
                                                   final Reader reader) {
    final Tokenizer sink = new NumericTokenizer(reader, new FloatNumericParser(), precisionStep);
    return new TokenStreamComponents(sink);
  }

  @Override
  public FloatNumericParser getNumericParser() {
    return new FloatNumericParser();
  }

  public class FloatNumericParser extends NumericParser<Float> {

    @Override
    public long parseAndConvert(final Reader input) throws IOException {
      return NumericUtils.floatToSortableInt(this.parse(input));
    }

    @Override
    public NumericType getNumericType() {
      return NumericType.FLOAT;
    }

    @Override
    public int getValueSize() {
      return 32;
    }

    @Override
    public Float parse(final Reader input)
    throws IOException {
      return XSDPrimitiveTypeParser.parseFloat(input);
    }

  }


}
