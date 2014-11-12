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

import com.sindicetech.siren.analysis.filter.PositionAttributeFilter;
import com.sindicetech.siren.analysis.filter.SirenPayloadFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class MockSirenAnalyzer extends Analyzer {

  public MockSirenAnalyzer() {}

  @Override
  protected TokenStreamComponents createComponents(final String fieldName,
                                                   final Reader reader) {
    final MockSirenReader mockReader = (MockSirenReader) reader;
    final MockSirenTokenizer tokenizer = new MockSirenTokenizer(mockReader);

    TokenStream sink = new PositionAttributeFilter(tokenizer);
    sink = new SirenPayloadFilter(sink);
    return new TokenStreamComponents(tokenizer, sink);
  }

  public TokenStream tokenStream() throws IOException {
    return this.tokenStream("", new StringReader(""));
  }

}
