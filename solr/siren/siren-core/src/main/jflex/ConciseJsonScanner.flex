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

import static com.sindicetech.siren.analysis.ExtendedJsonTokenizer.*;

import java.util.Stack;
import java.util.Arrays;
import java.util.Deque;
import java.util.ArrayDeque;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IntsRef;
import com.sindicetech.siren.util.ArrayUtils;
import com.sindicetech.siren.util.XSDDatatype;
import com.sindicetech.siren.util.JSONDatatype;

/**
 * A scanner for JSON document that maps a JSON tree to a concise tree model using a depth-first traversal approach.
 * <p>
 * A concise tree model creates one token per attribute-object and attribute-value pair. A token represents the content
 * of a node. The attribute-object token has an empty content. To be able to distinguish between multiple objects, it
 * duplicates the attribute-object token for each attribute-object pair and increments its node identifiers.
 * Nested arrays are not flattened and will be mapped to an empty node.
 */
%%
%yylexthrow java.lang.IllegalStateException
%public
%final
%class ConciseJsonScanner

/**
 * Both options cause the generated scanner to use the full 16 bit
 * Unicode input character set (character codes 0-65535).
 */
%unicode

/**
 * The current character number with the variable yychar.
 */
%char

/**
 * Both cause the scanning method to be declared as of Java type int.
 * Actions in the specification can then return int values as tokens
 */
%integer

/**
 * causes JFlex to compress the generated DFA table and to store it
 * in one or more string literals.
 */
%pack

%function getNextToken

%{

  /**
   * Buffer containing literal or number value
   */
  private final StringBuilder buffer = new StringBuilder();

  // OBJECT

  /**
   * Flag to indicate if the current object is empty, i.e., if it does not contain at least one field
   */
  private boolean isObjectEmpty = true;

  // NODE IDENTIFIERS

  /**
   * The size of the path buffer
   */
  private final static int BUFFER_SIZE = 1024;

  /**
   * The path to a node
   */
  private final IntsRef nodePath = new IntsRef(BUFFER_SIZE);

  /**
   * Stack of lexical states.
   * <p>
   * It is used to (1) verify that objects and arrays are correctly closed, and (2) to be able to resume the previous
   * state during the traversal of the tree.
   */
  private final Deque<Integer> states = new ArrayDeque<Integer>();

  // DATATYPES

  /**
   * Datatype representing xsd:string
   */
  private static final char[] XSD_STRING = XSDDatatype.XSD_STRING.toCharArray();

  /**
   * Datatype representing json:field
   */
  private static final char[] JSON_FIELD = JSONDatatype.JSON_FIELD.toCharArray();

  /**
   * Datatype representing xsd:double
   */
  private static final char[] XSD_DOUBLE = XSDDatatype.XSD_DOUBLE.toCharArray();

  /**
   * Datatype representing xsd:long
   */
  private static final char[] XSD_LONG = XSDDatatype.XSD_LONG.toCharArray();

  /**
   * Datatype representing xsd:boolean
   */
  private static final char[] XSD_BOOLEAN = XSDDatatype.XSD_BOOLEAN.toCharArray();

  /**
   * Buffer used to build the datatype label
   */
  private final StringBuilder dtLabel = new StringBuilder();

  /**
   * A reference to the current datatype URI
   */
  private char[] datatype;

  /**
   * Flags to indicate if the datatype object has a datatype label and value.
   */
  private boolean hasLabel, hasValue;

  // FIELDS

  /**
   * Buffer used to build the field label
   */
  private final StringBuilder fieldLabel = new StringBuilder();

  /**
   * Queue of field labels
   */
  private final Deque<String> paths = new ArrayDeque<String>();

  // ARRAY

  private boolean mustFlattenNestedArray = false;

  /**
   * If set to true, the nested arrays will be flattened.
   */
  public void setMustFlattenNestedArray(boolean mustFlattenNestedArray) {
    this.mustFlattenNestedArray = mustFlattenNestedArray;
  }

  /**
   * Return a copy of the current path.
   */
  public final String[] getPath() {
    return paths.toArray(new String[paths.size()]);
  }

  /**
   * Return the datatype URI.
   */
  public final char[] getDatatypeURI() {
    return datatype;
  }

  public final int yychar() {
    return yychar;
  }

  /**
   * Fills Lucene TermAttribute with the current string buffer.
   */
  public final void getLiteralText(CharTermAttribute t) {
    char[] chars = new char[buffer.length()];
    buffer.getChars(0, buffer.length(), chars, 0);
    t.copyBuffer(chars, 0, chars.length);
  }

  /**
   * Returns the node path of the token
   */
  public final IntsRef getNodePath() {
    return nodePath;
  }

  /**
   * Initialise inner variables
   */
  private void reset() {
    states.clear();
    Arrays.fill(nodePath.ints, -1);
    nodePath.offset = 0;
    nodePath.length = 0;
    datatype = null;
  }

  /**
   * Helper method to print an error while scanning a JSON
   * document with line and column information.
   */
  private String errorMessage(String msg) {
    return "Error parsing JSON document at [line=" + yyline + ", column=" + yycolumn + "]: " + msg;
  }

  /**
   * Called when entering into a new object. Adds a {@link #sOBJECT} state to the stack. Set the flag for empty object
   * to true.
   */
  private void newObject() {
    states.push(sOBJECT);
    this.incrNodeObjectPath();
    this.isObjectEmpty = true;
  }

  /**
   * Add an object to the current node path
   */
  private void incrNodeObjectPath() {
    ArrayUtils.growAndCopy(nodePath, nodePath.length + 1);
    nodePath.length++;
    // Initialise node
    setLastNode(-1);
  }

  /**
   * Update the path of the current values of the current object node
   */
  private void setLastNode(int val) {
    nodePath.ints[nodePath.length - 1] = val;
  }

  /**
   * Called when leaving an object:
   * <ul>
   * <li> decrement the tree level of 1; and
   * <li> remove {@link #sOBJECT} from the stack of states.
   * </ul>
   */
  private void closeObject() {
    final int state = states.pop();
    if (state != sOBJECT) {
      throw new IllegalStateException(errorMessage("Object is not properly closed"));
    }

    decrNodeObjectPath();
  }

  /**
   * Decrement the tree level of 1.
   */
  private void decrNodeObjectPath() {
    nodePath.length--;
  }

  /**
   * Called for each new number value. Return the {@link #NUMBER} token type.
   */
  private int newNumber(char[] datatype) {
    this.datatype = datatype;
    buffer.setLength(0);
    buffer.append(yytext());
    return NUMBER;
  }

  /**
   * Called for each new boolean value. Return the {@link #NUMBER} token type.
   */
  private int newBoolean(boolean b) {
    this.datatype = XSD_BOOLEAN;
    return b ? TRUE : FALSE;
  }

  /**
   * Update the path of the current values of the current object node
   */
  private void addToLastNode(int val) {
    nodePath.ints[nodePath.length - 1] += val;
  }

%}

TRUE            = "true"
FALSE           = "false"
LONG            = -?[0-9][0-9]*({EXPONENT})?
DOUBLE          = -?[0-9][0-9]*\.[0-9]+({EXPONENT})?
EXPONENT        = [e|E][+|-]?[0-9]+
NULL            = "null"
ENDOFLINE       = \r|\n|\r\n
WHITESPACE      = {ENDOFLINE} | [ \t\f]
// Datatype object fields
DT_LABEL        = "_datatype_"
DT_VALUE        = "_value_"
// Restricted object fields
RESTRICTED_FLD  = {DT_LABEL} | {DT_VALUE}

%xstate sOBJECT, sFIELD, sVALUE, sSTRING, sARRAY
%xstate sDATATYPE, sDATATYPE_FIELD, sDATATYPE_LABEL, sDATATYPE_VALUE

%%

<YYINITIAL> {

  "{"                            {
                                    reset();
                                    yypushback(1);
                                    yybegin(sOBJECT);
                                 }

  {WHITESPACE}                   { /* ignore white space. */ }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning start of the json input: [" + yytext() + "]")); }

}

// Parse a JSON object
<sOBJECT> {

  "{"                            {
                                    this.newObject();
                                 }

  \"                             {
                                    this.isObjectEmpty = false;
                                    fieldLabel.setLength(0);
                                    yybegin(sFIELD);
                                 }

  \" {RESTRICTED_FLD}            {
                                    // Throws exception if object uses restricted field names
                                    throw new IllegalStateException(errorMessage("Restricted field name found while scanning an object: [" + yytext() + "]"));
                                 }

  "}"                            {
                                    if (!isObjectEmpty) {
                                      paths.removeLast();
                                    }

                                    this.closeObject();

                                    // while stack of states is non empty, unroll states
                                    if (!states.isEmpty()) {
                                      yybegin(states.peek());
                                    }
                                 }

  ","                            {
                                    if (!isObjectEmpty) {
                                      paths.removeLast();
                                    }
                                 }

  {WHITESPACE}                   { /* ignore white space. */ }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning an object: [" + yytext() + "]")); }

}

// Parse a field of a JSON object
<sFIELD> {

  \" {WHITESPACE}* ":"           {
                                    paths.addLast(fieldLabel.toString());
                                    yybegin(sVALUE);
                                 }

  [^\"\\]+                       {  fieldLabel.append(yytext()); }

  \\\"                           {  fieldLabel.append('\"'); }

  \\.                            {  fieldLabel.append(yytext()); }

  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] {
                                    fieldLabel.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16)));
                                 }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning a field: [" + yytext() + "]")); }

}

// Parse a value
<sVALUE> {

  {NULL}                         {
                                    datatype = XSD_STRING;
                                    addToLastNode(1);
                                    yybegin(states.peek());
                                    return NULL;
                                 }

  {FALSE}                        {
                                    addToLastNode(1);
                                    yybegin(states.peek());
                                    return newBoolean(false);
                                 }

  {TRUE}                         {
                                    addToLastNode(1);
                                    yybegin(states.peek());
                                    return newBoolean(true);
                                 }

  {LONG}                         {
                                    addToLastNode(1);
                                    yybegin(states.peek());
                                    return newNumber(XSD_LONG);
                                 }

  {DOUBLE}                       {
                                    addToLastNode(1);
                                    yybegin(states.peek());
                                    return newNumber(XSD_DOUBLE);
                                 }

  \"                             {
                                    datatype = XSD_STRING;
                                    addToLastNode(1);
                                    buffer.setLength(0);
                                    yybegin(sSTRING);
                                 }

  "{"                            {
                                    addToLastNode(1);
                                    yypushback(1);
                                    yybegin(sOBJECT);

                                    // return an empty token for the field
                                    datatype = XSD_STRING;
                                    buffer.setLength(0);
                                    return LITERAL;
                                 }

  "{" / {WHITESPACE}* \" ( {DT_LABEL} | {DT_VALUE} ) \"
                                 {
                                    addToLastNode(1);
                                    yypushback(1);
                                    yybegin(sDATATYPE);
                                 }

  "["                            {
                                    yypushback(1);
                                    yybegin(sARRAY);
                                 }

  {WHITESPACE}                   { /* ignore white space. */ }

}

// Parse a string value
<sSTRING> {

  \"                             {
                                    yybegin(states.peek());
                                    return LITERAL;
                                 }

  [^\"\\]+                       {  buffer.append(yytext()); }

  \\\"                           {  buffer.append('\"'); }

  \\.                            {  buffer.append(yytext()); }

  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] {
                                    buffer.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16)));
                                 }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning a string: [" + yytext() + "]")); }

}

// Parse an array
<sARRAY> {

  "["                            {
                                    // if the previous state is an array, this is a nested array,
                                    // we need to increase the node identifier
                                    if (!mustFlattenNestedArray && states.peek() == sARRAY) {
                                      addToLastNode(1);
                                      this.incrNodeObjectPath();
                                    }
                                    states.push(sARRAY);
                                    yybegin(sVALUE);
                                 }


  <sVALUE> // Share this expression with sVALUE in order to allow empty array
  "]"                            {
                                    int state = states.pop();
                                    if (state != sARRAY) {
                                      throw new IllegalStateException(errorMessage("Array is not properly closed"));
                                    }
                                    state = states.peek();
                                    if (!mustFlattenNestedArray && state == sARRAY) { // we were in a nested array, decrease the node identifier
                                      this.decrNodeObjectPath();
                                    }
                                    yybegin(state);
                                 }

  ","                            {
                                    yybegin(sVALUE);
                                 }

  {WHITESPACE}                   { /* ignore white space. */ }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning an array: [" + yytext() + "]")); }

}

// Parse a datatype object
<sDATATYPE> {

  "{"                            {
                                    hasLabel = hasValue = false;
                                    states.push(sDATATYPE);
                                    yybegin(sDATATYPE_FIELD);
                                 }

  "}"                            {
                                    if (!hasLabel) {
                                      throw new IllegalStateException(errorMessage("Invalid datatype object: no _datatype_ attribute."));
                                    }
                                    if (!hasValue) {
                                      throw new IllegalStateException(errorMessage("Invalid datatype object: no _value_ attribute."));
                                    }
                                    int state = states.pop();
                                    if (state != sDATATYPE) {
                                      throw new IllegalStateException(errorMessage("Datatype object is not properly closed"));
                                    }
                                    yybegin(states.peek());
                                    return LITERAL;
                                 }

  ","                            {
                                    yybegin(sDATATYPE_FIELD);
                                 }

  {WHITESPACE}                   { /* ignore white space. */ }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning a datatype: [" + yytext() + "]")); }

}

// Parse the fields of the datatype object
<sDATATYPE_FIELD> {

  // Datatype label
  \" {DT_LABEL} \" {WHITESPACE}* ":" {WHITESPACE}* \"
                                 {
                                    hasLabel = true;
                                    dtLabel.setLength(0);
                                    yybegin(sDATATYPE_LABEL);
                                 }

  // Datatype value: a string
  \" {DT_VALUE} \" {WHITESPACE}* ":" {WHITESPACE}* \"
                                 {
                                    hasValue = true;
                                    buffer.setLength(0);
                                    yybegin(sDATATYPE_VALUE);
                                 }

  {WHITESPACE}                   { /* ignore white space. */ }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning a datatype: [" + yytext() + "]")); }

}

// Parse a datatype label
<sDATATYPE_LABEL> {

  \"                             {
                                    datatype = new char[dtLabel.length()];
                                    dtLabel.getChars(0, dtLabel.length(), datatype, 0);
                                    yybegin(sDATATYPE);
                                 }

  [^\"\\]+                       {  dtLabel.append(yytext()); }

  \\\"                           {  dtLabel.append('\"'); }

  \\.                            {  dtLabel.append(yytext()); }

  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] {
                                    dtLabel.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16)));
                                 }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning a datatype label: [" + yytext() + "]")); }

}

// Parse a datatype value. Similar to sSTRING, but does not return a literal
<sDATATYPE_VALUE> {

  \"                             {
                                    yybegin(sDATATYPE);
                                 }

  [^\"\\]+                       {  buffer.append(yytext()); }

  \\\"                           {  buffer.append('\"'); }

  \\.                            {  buffer.append(yytext()); }

  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] {
                                    buffer.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16)));
                                 }

  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning a datatype string: [" + yytext() + "]")); }

}

// Check that the stack of states is empty
<<EOF>>                          {
                                    if (!states.isEmpty()) {
                                      throw new IllegalStateException(errorMessage("Arrays/objects/strings are not properly closed"));
                                    }
                                    return YYEOF;
                                 }