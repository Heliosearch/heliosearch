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

package com.sindicetech.siren.analysis.filter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.CharArrayMap;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sindicetech.siren.analysis.attributes.DatatypeAttribute;
import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.PathAttribute;
import com.sindicetech.siren.util.JSONDatatype;
import com.sindicetech.siren.util.ReusableCharArrayReader;
import com.sindicetech.siren.util.XSDDatatype;

import java.io.IOException;

/**
 * This class performs post-processing operation on the tokens extracted by
 * {@link com.sindicetech.siren.analysis.ExtendedJsonTokenizer} based on the {@link DatatypeAttribute}.
 * <p>
 * This filter provides a {@link #register(char[], Analyzer)} method which allows
 * to register an {@link Analyzer} to a specific datatype.
 */
public class DatatypeAnalyzerFilter extends TokenFilter {

  private final static Logger logger =
    LoggerFactory.getLogger(DatatypeAnalyzerFilter.class);

  private final CharArrayMap<Analyzer> dtsAnalyzer;

  private CharTermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private PositionIncrementAttribute posIncrAtt;
  private TypeAttribute typeAtt;
  private DatatypeAttribute dtypeAtt;
  private NodeAttribute nodeAtt;
  private PathAttribute pathAtt;

  private CharTermAttribute tokenTermAtt;
  private OffsetAttribute tokenOffsetAtt;
  private PositionIncrementAttribute tokenPosIncrAtt;
  private TypeAttribute tokenTypeAtt;

  private boolean isConsumingToken = false;
  private TokenStream currentStream;

  private ReusableCharArrayReader reusableCharArray;

  public DatatypeAnalyzerFilter(final TokenStream input) {
    super(input);
    // here, we just need to indicate a version > Lucene 3.1 - see CharArrayMap
    dtsAnalyzer = new CharArrayMap<Analyzer>(Version.LUCENE_46, 64, false);
    this.initAttributes();
  }

  /**
   * Create a {@link DatatypeAnalyzerFilter} with the given default
   * {@link Analyzer}s for the {@link JSONDatatype#JSON_FIELD} and
   * {@link XSDDatatype#XSD_STRING}.
   *
   * @param input the input token stream
   * @param fieldAnalyzer the default field name {@link Analyzer}
   * @param valueAnalyzer the default value {@link Analyzer}
   */
  public DatatypeAnalyzerFilter(final TokenStream input,
                                final Analyzer fieldAnalyzer,
                                final Analyzer valueAnalyzer) {
    this(input);
    // register the default analyzers
    this.register(XSDDatatype.XSD_STRING.toCharArray(), valueAnalyzer);
    this.register(JSONDatatype.JSON_FIELD.toCharArray(), fieldAnalyzer);
  }

  /**
   * Initialise the attributes of the main stream
   */
  private void initAttributes() {
    termAtt = input.getAttribute(CharTermAttribute.class);
    offsetAtt = input.getAttribute(OffsetAttribute.class);
    posIncrAtt = input.getAttribute(PositionIncrementAttribute.class);
    typeAtt = input.getAttribute(TypeAttribute.class);
    dtypeAtt = input.getAttribute(DatatypeAttribute.class);
    nodeAtt = this.addAttribute(NodeAttribute.class);
    pathAtt = this.addAttribute(PathAttribute.class);
  }

  /**
   * Initialise the attributes of the inner stream used to tokenize the incoming token.
   */
  private void initTokenAttributes() {
    tokenTermAtt = currentStream.addAttribute(CharTermAttribute.class);
    tokenOffsetAtt = currentStream.addAttribute(OffsetAttribute.class);
    tokenPosIncrAtt = currentStream.addAttribute(PositionIncrementAttribute.class);
    tokenTypeAtt = currentStream.addAttribute(TypeAttribute.class);
  }

  /**
   * Map the given analyzer to that dataTypeURI
   */
  public void register(final char[] dataTypeURI, final Analyzer analyzer) {
    if (!dtsAnalyzer.containsKey(dataTypeURI)) {
      dtsAnalyzer.put(dataTypeURI, analyzer);
    }
  }

  @Override
  public final boolean incrementToken()
  throws IOException {
    /*
     * the use of the loop is necessary in the case where it was consuming a token
     * but that token stream reached the end, and so incrementToken return false.
     * The loop makes sure that the next token is processed.
     */
    do {
      if (!isConsumingToken) {
        if (!input.incrementToken()) {
          return false;
        }

        // If the term is empty, this must be an empty token from the concise tokenizer
        // we skip the datatype analysis, because it might be removed by the datatype analyzers.
        if (termAtt.length() == 0) {
          return true;
        }

        final char[] dt = dtypeAtt.datatypeURI();
        if (dt == null || dt.length == 0) { // empty datatype, e.g., a bnode
          // TODO GH-164
          logger.warn("Empty datatype for the token [{}]", termAtt);
          return true;
        }

        // the datatype is not registered, leave the token as it is
        if (!dtsAnalyzer.containsKey(dt)) {
          throw new IOException("Unregistered datatype [" + new String(dt)
            + "]. Use the #register method.");
        }

        final Analyzer analyzer = dtsAnalyzer.get(dt);
        if (reusableCharArray == null) {
          reusableCharArray = new ReusableCharArrayReader(termAtt.buffer(), 0, termAtt.length());
        } else {
          reusableCharArray.reset(termAtt.buffer(), 0, termAtt.length());
        }
        currentStream = analyzer.tokenStream("", reusableCharArray);
        currentStream.reset(); // reset to prepare the stream for consumption
        this.initTokenAttributes();
      }
      // Consume the token with the registered analyzer
      isConsumingToken = currentStream.incrementToken();

      // If the token is consumed, perform end of stream operation and close the stream
      if (!isConsumingToken) {
        currentStream.end();
        currentStream.close();
      }
    } while (!isConsumingToken);
    this.copyInnerStreamAttributes();
    return true;
  }

  /**
   * Copy the inner's stream attributes values to the main stream's ones. This filter
   * uses an inner stream, therefore it needs to be cleared so that other filters
   * have clean attributes data. Because of that, the attributes datatypeURI and
   * node have to saved in order to be restored after.
   */
  private void copyInnerStreamAttributes() {
    // backup datatype, node identifier and path
    final IntsRef nodeId = IntsRef.deepCopyOf(nodeAtt.node());
    final char[] dt = dtypeAtt.datatypeURI();
    final String[] path = pathAtt.path();

    // clear attributes
    input.clearAttributes();

    // copy inner attributes
    final int len = tokenTermAtt.length();
    termAtt.copyBuffer(tokenTermAtt.buffer(), 0, len);
    offsetAtt.setOffset(tokenOffsetAtt.startOffset(), tokenOffsetAtt.endOffset());
    posIncrAtt.setPositionIncrement(tokenPosIncrAtt.getPositionIncrement());
    typeAtt.setType(tokenTypeAtt.type());
    // TupleTokenizer handles the setting of tuple/cell values and the datatype URI

    // restore datatype, node and path attributes
    nodeAtt.copyNode(nodeId);
    dtypeAtt.setDatatypeURI(dt);
    pathAtt.setPath(path);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

}
