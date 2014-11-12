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

import org.apache.lucene.util.AttributeFactory;

import java.io.IOException;
import java.io.Reader;

/**
 * A tokenizer for JSON documents.
 * <p>
 * The tokenizer parses JSON data and generates a token for each field name, object and value.
 *
 * @see com.sindicetech.siren.analysis.AbstractJsonTokenizer
 * @see ExtendedJsonScanner
 */
public class ExtendedJsonTokenizer extends AbstractJsonTokenizer {

  private final ExtendedJsonScanner scanner;

  public ExtendedJsonTokenizer(final Reader input) {
    super(input);
    scanner = new ExtendedJsonScanner(input);
  }

  public ExtendedJsonTokenizer(final AttributeFactory factory, final Reader input) {
    super(factory, input);
    scanner = new ExtendedJsonScanner(input);
  }

  @Override
  protected boolean nextToken() throws IOException {
    final int tokenType = scanner.getNextToken();

    switch (tokenType) {
      case FALSE:
        termAtt.append("false");
        this.updateToken(tokenType, scanner.yychar());
        break;

      case TRUE:
        termAtt.append("true");
        this.updateToken(tokenType, scanner.yychar());
        break;

      case NULL:
        termAtt.append("null");
        this.updateToken(tokenType, scanner.yychar());
        break;

      case NUMBER:
        scanner.getLiteralText(termAtt);
        this.updateToken(tokenType, scanner.yychar());
        break;

      case LITERAL:
        scanner.getLiteralText(termAtt);
        this.updateToken(tokenType, scanner.yychar() + 1);
        break;

      default:
        return false;
    }
    return true;
  }

  /**
   * Update type, datatype, offset, tuple id and cell id of the token
   *
   * @param tokenType The type of the generated token
   * @param startOffset The starting offset of the token
   */
  private void updateToken(final int tokenType, final int startOffset) {
    // Update offset
    offsetAtt.setOffset(this.correctOffset(startOffset),
      this.correctOffset(startOffset + termAtt.length()));
    // update token type
    typeAtt.setType(TOKEN_TYPES[tokenType]);
    // update datatype
    dtypeAtt.setDatatypeURI(scanner.getDatatypeURI());
    // Update structural information
    nodeAtt.copyNode(scanner.getNodePath());
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    if (input.markSupported()) {
      input.reset();
    }
    scanner.yyreset(input);
  }

  @Override
  public void close() throws IOException {
    super.close();
    scanner.yyclose();
  }

}
