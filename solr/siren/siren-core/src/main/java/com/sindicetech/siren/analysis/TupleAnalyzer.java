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
import org.apache.lucene.analysis.util.CharArrayMap;
import org.apache.lucene.util.Version;

import com.sindicetech.siren.analysis.filter.DatatypeAnalyzerFilter;
import com.sindicetech.siren.analysis.filter.PositionAttributeFilter;
import com.sindicetech.siren.analysis.filter.SirenPayloadFilter;
import com.sindicetech.siren.analysis.filter.TokenTypeFilter;

import java.io.Reader;
import java.util.Map.Entry;

/**
 * The TupleAnalyzer is especially designed to process RDF data. It applies
 * various post-processing on URIs and Literals.
 *
 * @deprecated Use {@link ExtendedJsonAnalyzer} instead
 */
@Deprecated
public class TupleAnalyzer extends Analyzer {

  private Analyzer stringAnalyzer;
  private Analyzer anyURIAnalyzer;

  private final Version matchVersion;

  private final CharArrayMap<Analyzer> regLitAnalyzers;

  /**
   * Create a {@link TupleAnalyzer} with the default {@link Analyzer} for Literals and URIs.
   * @param version
   * @param stringAnalyzer default Literal {@link Analyzer}
   * @param anyURIAnalyzer default URI {@link Analyzer}
   */
  public TupleAnalyzer(final Version version, final Analyzer stringAnalyzer, final Analyzer anyURIAnalyzer) {
    matchVersion = version;
    this.stringAnalyzer = stringAnalyzer;
    this.anyURIAnalyzer = anyURIAnalyzer;
    regLitAnalyzers = new CharArrayMap<Analyzer>(version, 64, false);

  }

  public void setLiteralAnalyzer(final Analyzer analyzer) {
    stringAnalyzer = analyzer;
  }

  public void setAnyURIAnalyzer(final Analyzer analyzer) {
    anyURIAnalyzer = analyzer;
  }

  /**
   * Assign an {@link Analyzer} to be used with that key. That analyzer is used
   * to process tokens outputed from the {@link TupleTokenizer}.
   * @param datatype the Datatype
   * @param a the associated {@link Analyzer}
   */
  public void registerDatatype(final char[] datatype, final Analyzer a) {
    if (!regLitAnalyzers.containsKey(datatype)) {
      regLitAnalyzers.put(datatype, a);
    }
  }

  /**
   * Remove all registered Datatype {@link Analyzer}s.
   */
  public void clearDatatypes() {
    regLitAnalyzers.clear();
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    final TupleTokenizer source = new TupleTokenizer(reader);

    TokenStream sink = new TokenTypeFilter(matchVersion, source, new int[] {TupleTokenizer.BNODE,
                                                                            TupleTokenizer.DOT});
    final DatatypeAnalyzerFilter tt = new DatatypeAnalyzerFilter(sink, anyURIAnalyzer, stringAnalyzer);
    for (final Entry<Object, Analyzer> e : regLitAnalyzers.entrySet()) {
      tt.register((char[]) e.getKey(), e.getValue());
    }
    sink = new PositionAttributeFilter(tt);
    sink = new SirenPayloadFilter(sink);
    return new TokenStreamComponents(source, sink);
  }

}
