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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

import com.sindicetech.siren.analysis.filter.DatatypeAnalyzerFilter;
import com.sindicetech.siren.analysis.filter.PathEncodingFilter;
import com.sindicetech.siren.analysis.filter.PositionAttributeFilter;
import com.sindicetech.siren.analysis.filter.SirenPayloadFilter;

import java.io.Reader;
import java.util.Map.Entry;

/**
 * The {@link ConciseJsonAnalyzer} is designed to process JSON documents and maps them to an internal concise tree model.
 */
public class ConciseJsonAnalyzer extends ExtendedJsonAnalyzer {

  private boolean generateTokensWithoutPath = false;

  /**
   * Create a {@link ConciseJsonAnalyzer} with the specified
   * {@link org.apache.lucene.analysis.Analyzer}s for
   * field names and values.
   * <p>
   * The default analyzer for field names will be associated with the datatype
   * {@link com.sindicetech.siren.util.JSONDatatype#JSON_FIELD}. The default analyzer for values will be
   * associated with the datatype {@link com.sindicetech.siren.util.XSDDatatype#XSD_STRING}.
   *
   * @param fieldAnalyzer Default {@link org.apache.lucene.analysis.Analyzer} for the field names
   * @param valueAnalyzer Default {@link org.apache.lucene.analysis.Analyzer} for the values
   */
  public ConciseJsonAnalyzer(final Analyzer fieldAnalyzer,
                             final Analyzer valueAnalyzer) {
    super(fieldAnalyzer, valueAnalyzer);
  }

  public void setGenerateTokensWithoutPath(boolean withoutPath) {
    this.generateTokensWithoutPath = withoutPath;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    final ConciseJsonTokenizer source = new ConciseJsonTokenizer(reader);

    final DatatypeAnalyzerFilter tt = new DatatypeAnalyzerFilter(source, fieldAnalyzer, valueAnalyzer);
    for (final Entry<Object, Analyzer> e : regAnalyzers.entrySet()) {
      tt.register((char[]) e.getKey(), e.getValue());
    }

    PathEncodingFilter pathEncodingFilter = new PathEncodingFilter(tt);
    pathEncodingFilter.setPreserveOriginal(this.generateTokensWithoutPath);

    TokenStream sink = new PositionAttributeFilter(pathEncodingFilter);
    sink = new SirenPayloadFilter(sink);
    return new TokenStreamComponents(source, sink);
  }

}
