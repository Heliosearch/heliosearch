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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import com.sindicetech.siren.analysis.attributes.DatatypeAttribute;
import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import com.sindicetech.siren.analysis.attributes.TupleNodeAttributeImpl;

import java.io.IOException;
import java.io.Reader;

/**
 * A grammar-based tokenizer constructed with JFlex for N-Tuples. Splits a
 * N-Tuple into BNode, URI, Literal and Dot tokens.
 *
 * @deprecated Use {@link ExtendedJsonTokenizer} instead
 */
@Deprecated
public class TupleTokenizer extends Tokenizer {

  /** A private instance of the JFlex-constructed scanner */
  private final TupleTokenizerImpl _scanner;

  /** Structural node counters */
  private int                      _tid = 0;

  private int                      _cid = 0;

  /** Token definition */

  public static final int          BNODE                = 0;

  public static final int          URI                  = 1;

  public static final int          LITERAL              = 2;

  public static final int          DOT                  = 3;

  protected static String[]        TOKEN_TYPES;

  public static String[] getTokenTypes() {
    if (TOKEN_TYPES == null) {
      TOKEN_TYPES = new String[4];
      TOKEN_TYPES[BNODE] = "<BNODE>";
      TOKEN_TYPES[URI] = "<URI>";
      TOKEN_TYPES[LITERAL] = "<LITERAL>";
      TOKEN_TYPES[DOT] = "<DOT>";
    }
    return TOKEN_TYPES;
  }

  /**
   * Creates a new instance of the {@link TupleTokenizer}. Attaches the
   * <code>input</code> to a newly created JFlex scanner.
   */
  public TupleTokenizer(final Reader input) {
    super(input);
    this._scanner = new TupleTokenizerImpl(input);
    this.initAttributes();
  }

  // the TupleTokenizer generates 6 attributes:
  // term, offset, positionIncrement, type, datatype, node
  private CharTermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private PositionIncrementAttribute posIncrAtt;
  private TypeAttribute typeAtt;
  private DatatypeAttribute dtypeAtt;
  private NodeAttribute nodeAtt;

  private void initAttributes() {
    termAtt = this.addAttribute(CharTermAttribute.class);
    offsetAtt = this.addAttribute(OffsetAttribute.class);
    posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);
    typeAtt = this.addAttribute(TypeAttribute.class);
    dtypeAtt = this.addAttribute(DatatypeAttribute.class);
    if (!this.hasAttribute(NodeAttribute.class)) {
      this.addAttributeImpl(new TupleNodeAttributeImpl());
    }
    nodeAtt = this.addAttribute(NodeAttribute.class);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    this.clearAttributes();
    posIncrAtt.setPositionIncrement(1);
    return this.nextTupleToken();
  }

  private boolean nextTupleToken() throws IOException {
    final int tokenType = _scanner.getNextToken();

    switch (tokenType) {
      case TupleTokenizer.BNODE:
        _scanner.getBNodeText(termAtt);
        this.updateToken(tokenType, null, _scanner.yychar() + 2);
        // Increment tuple cell ID counter
        _cid++;
        break;

      case TupleTokenizer.URI:
        _scanner.getURIText(termAtt);
        this.updateToken(tokenType, _scanner.getDatatypeURI(), _scanner.yychar() + 1);
        // Increment tuple cell ID counter
        _cid++;
        break;

      case TupleTokenizer.LITERAL:
        _scanner.getLiteralText(termAtt);
        this.updateToken(tokenType, _scanner.getDatatypeURI(), _scanner.yychar() + 1);
        // Increment tuple cell ID counter
        _cid++;
        break;

      case DOT:
        _scanner.getText(termAtt);
        this.updateToken(tokenType, null, _scanner.yychar());
        // Increment tuple ID counter, reset tuple cell ID counter
        _tid++; _cid = 0;
        break;

      case TupleTokenizerImpl.YYEOF:
        return false;

      default:
        return false;
    }
    return true;
  }

  /**
   * Update type, datatype, offset, tuple id and cell id of the token
   *
   * @param tokenType The type of the generated token
   * @param datatypeURI The datatype of the generated token
   * @param startOffset The starting offset of the token
   */
  private void updateToken(final int tokenType, final char[] datatypeURI, final int startOffset) {
    // Update offset
    offsetAtt.setOffset(this.correctOffset(startOffset),
      this.correctOffset(startOffset + termAtt.length()));
    // update token type
    typeAtt.setType(TOKEN_TYPES[tokenType]);
    // update datatype
    dtypeAtt.setDatatypeURI(datatypeURI);
    // Update structural information
    nodeAtt.append(_tid);
    nodeAtt.append(_cid);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.analysis.TokenStream#reset()
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    if (input.markSupported()) {
      input.reset();
    }
    _scanner.yyreset(input);
    _tid = _cid = 0;
  }

  @Override
  public void close() throws IOException {
    super.close();
    _scanner.yyclose();
  }

}
