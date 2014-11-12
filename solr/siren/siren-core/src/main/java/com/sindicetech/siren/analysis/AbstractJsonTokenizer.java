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

import com.sindicetech.siren.analysis.attributes.DatatypeAttribute;
import com.sindicetech.siren.analysis.attributes.JsonNodeAttributeImpl;
import com.sindicetech.siren.analysis.attributes.NodeAttribute;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeFactory;
import java.io.IOException;
import java.io.Reader;

/**
 * Abstraction over the tokenizers for JSON documents. The tokenizer is mapping the JSON model to a SIREn's tree model.
 * <p>
 * The tokenizer returns a token per node in the tree. It attaches to each token a node identifier using the
 * {@link NodeAttribute} and a datatype using the {@link DatatypeAttribute}.
 * <p>
 * It is mandatory for the tokenizer to return tokens following the natural order of the node identifiers.
 * <p>
 * Regarding datatype, the convention is the following:
 * <ul>
 *   <li> If a field name is parsed, the datatype
 *        {@link com.sindicetech.siren.util.JSONDatatype#JSON_FIELD} is assigned;
 *   <li> If a value string is parsed, the datatype
 *        {@link com.sindicetech.siren.util.XSDDatatype#XSD_STRING} is assigned;
 *   <li> If a boolean value is parsed, the datatype
 *        {@link com.sindicetech.siren.util.XSDDatatype#XSD_BOOLEAN} is assigned;
 *   <li> If a numerical value is parsed, the datatype
 *        {@link com.sindicetech.siren.util.XSDDatatype#XSD_LONG} is assigned;
 *   <li> If a numerical value with a fraction is parsed, the datatype
 *        {@link com.sindicetech.siren.util.XSDDatatype#XSD_DOUBLE} is assigned;
 * </ul>
 */
public abstract class AbstractJsonTokenizer extends Tokenizer {

  /// Token Definition
  public static final int         NULL      = 0;
  public static final int         TRUE      = 1;
  public static final int         FALSE     = 2;
  public static final int         NUMBER    = 3;
  public static final int         LITERAL   = 4;

  /**
   * Datatype JSON schema: field for the datatype label
   */
  public static final String      DATATYPE_LABEL  = "_datatype_";

  /**
   * Datatype JSON schema: field for the datatype value
   */
  public static final String      DATATYPE_VALUES = "_value_";

  public AbstractJsonTokenizer(final Reader input) {
    super(input);
    this.initAttributes();
  }

  public AbstractJsonTokenizer(final AttributeFactory factory, final Reader input) {
    super(factory, input);
    this.initAttributes();
  }

  protected static String[] TOKEN_TYPES = getTokenTypes();

  public static String[] getTokenTypes() {
    if (TOKEN_TYPES == null) {
      TOKEN_TYPES = new String[5];
      TOKEN_TYPES[NULL] = "<NULL>";
      TOKEN_TYPES[TRUE] = "<TRUE>";
      TOKEN_TYPES[FALSE] = "<FALSE>";
      TOKEN_TYPES[NUMBER] = "<NUMBER>";
      TOKEN_TYPES[LITERAL] = "<LITERAL>";
    }
    return TOKEN_TYPES;
  }

  // A ExtendedJsonTokenizer contains at least 6 attributes:
  // term, offset, positionIncrement, type, datatype, node
  protected CharTermAttribute termAtt;
  protected OffsetAttribute offsetAtt;
  protected PositionIncrementAttribute posIncrAtt;
  protected TypeAttribute typeAtt;
  protected DatatypeAttribute dtypeAtt;
  protected NodeAttribute nodeAtt;

  private void initAttributes() {
    termAtt = this.addAttribute(CharTermAttribute.class);
    offsetAtt = this.addAttribute(OffsetAttribute.class);
    posIncrAtt = this.addAttribute(PositionIncrementAttribute.class);
    typeAtt = this.addAttribute(TypeAttribute.class);
    dtypeAtt = this.addAttribute(DatatypeAttribute.class);
    if (!this.hasAttribute(NodeAttribute.class)) {
      this.addAttributeImpl(new JsonNodeAttributeImpl());
    }
    nodeAtt = this.addAttribute(NodeAttribute.class);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    this.clearAttributes();
    posIncrAtt.setPositionIncrement(1);
    return this.nextToken();
  }

  /**
   * Advances to the next token and updates the attributes.
   */
  protected abstract boolean nextToken() throws IOException;

}
