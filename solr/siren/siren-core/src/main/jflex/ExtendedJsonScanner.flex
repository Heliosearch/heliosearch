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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IntsRef;
import com.sindicetech.siren.util.ArrayUtils;
import com.sindicetech.siren.util.XSDDatatype;
import com.sindicetech.siren.util.JSONDatatype;

/**
 * A scanner for JSON document that maps a JSON tree to a node-based tree model using a depth-first traversal approach.
 * <p>
 * This scanner creates one token per attribute, value and object. Nested arrays are not flattened and will be mapped
 * to an empty node.
 */
%%
%yylexthrow java.lang.IllegalStateException
%public
%final
%class ExtendedJsonScanner

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
  protected final StringBuilder buffer = new StringBuilder();

  // NODE IDENTIFIERS

  /**
   * The size of the path buffer
   */
  protected final static int BUFFER_SIZE = 1024;

  /**
   * The path to a node
   */
  protected final IntsRef nodePath = new IntsRef(BUFFER_SIZE);

  /**
   * Stack of lexical states
   */
  protected final Stack<Integer> states = new Stack();

  /**
   * Indicates if a leaf node, i.e., a literal, a number, null, or a boolean,
   * was encountered, in which case it needs to be closed, either in the COMMA
   * state, or in the closing curly bracket.
   */
  protected boolean openLeafNode = false;

  /**
   * Indicates how many nested objects there are.
   * A nested object implies a blank node.
   */
  protected int nestedObjects = 0;

  // DATATYPES

  /**
   * Datatype representing xsd:string
   */
  protected static final char[] XSD_STRING = XSDDatatype.XSD_STRING.toCharArray();

  /**
   * Datatype representing json:field
   */
  protected static final char[] JSON_FIELD = JSONDatatype.JSON_FIELD.toCharArray();

  /**
   * Datatype representing xsd:double
   */
  protected static final char[] XSD_DOUBLE = XSDDatatype.XSD_DOUBLE.toCharArray();

  /**
   * Datatype representing xsd:long
   */
  protected static final char[] XSD_LONG = XSDDatatype.XSD_LONG.toCharArray();

  /**
   * Datatype representing xsd:boolean
   */
  protected static final char[] XSD_BOOLEAN = XSDDatatype.XSD_BOOLEAN.toCharArray();

  /**
   * Buffer containing the datatype label
   */
  protected final StringBuilder dtLabel = new StringBuilder();

  /**
   * A reference to the current datatype URI
   */
  protected char[] datatype;

  // lexical states for datatypes
  protected final static int      DATATYPE_OBJ_OFF   = 0;
  protected final static int      DATATYPE_OBJ_ON    = 1;
  protected final static int      DATATYPE_OBJ_JUNK  = 2;
  protected final static int      DATATYPE_OBJ_ERROR = 3;
  protected final static int      DATATYPE_OBJ_LABEL = 4;
  protected final static int      DATATYPE_OBJ_VALUE = 8;
  protected final static int      DATATYPE_OBJ_OK    = 13;

  /**
   * The current lexical state for a datatype object
   */
  protected int datatypeObject = DATATYPE_OBJ_OFF;

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
    nodePath.length = 1;
    openLeafNode = false;
    datatype = null;
    nestedObjects = 0;
    datatypeObject = DATATYPE_OBJ_OFF;
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

  private void initDatatypeObject() {
    // Initialise the datatype data, this is possibly a Datatype object
    datatype = null;
    datatypeObject = DATATYPE_OBJ_OFF;
    buffer.setLength(0);
  }

  /**
   * Decrement the tree level of 1.
   */
  private void decrNodeObjectPath() {
    nodePath.length--;
  }

  /** Update the path of the current values of the current object node */
  private void setLastNode(int val) {
    nodePath.ints[nodePath.length - 1] = val;
  }

  /** Update the path of the current values of the current object node */
  private void addToLastNode(int val) {
    nodePath.ints[nodePath.length - 1] += val;
  }

  /**
   * Return the {@link #NUMBER} token type.
   */
  private int processNumber() {
    buffer.setLength(0);
    buffer.append(yytext());
    return NUMBER;
  }

  /**
   * Helper method to print an error while scanning a JSON
   * document with line and column information.
   */
  private String errorMessage(String msg) {
    return "Error parsing JSON document at [line=" + yyline + ", column=" + yycolumn + "]: " + msg;
  }

  /**
   * Close the JSON object:
   * - decrement the tree level of 1;
   * - decrement the tree level of 1 one more time if the last value was a leaf;
   * - decrement the tree level of 1 one more time if the object was nested into another one (i.e., the blank node),
   *   but exclude nested objects in arrays; and
   * - remove {@link #sOBJECT} from the stack of states.
   */
  private void closeObject() {
    final int state = states.pop();
    if (state != sOBJECT && state != sARRAY_OBJECT) {
      throw new IllegalStateException(errorMessage("Expected '}', got " + yychar()));
    }

    decrNodeObjectPath();
    if (openLeafNode) { // unclosed entry to a leaf node
      decrNodeObjectPath();
      openLeafNode = false;
    }

    // this curly bracket closes a nested object
    // this requires an additional call to decrNodeObjectPath()
    // since nested objects have implicitly a blank node.
    // This excludes the nested objects in array (i.e., sARRAY_OBJECT)
    if (state == sOBJECT && nestedObjects > 0) {
      nestedObjects--;
      decrNodeObjectPath();
    }

  }

  /**
   * Closes the JSON object and further checks if the node path should be updated.
   * It returns a {@link #LITERAL}, which is the value associated to the field "_value_"
   * with the datatype equal to the value  associated to the field "_datatype_".
   */
  private int closeDatatypeObject() {
    closeObject();
    // when the states stack is empty, I am the root of the JSON tree
    if (states.empty()) {
      throw new IllegalStateException(errorMessage("A datatype object at the" +
        " root of the JSON document is not possible."));
    }
    if (states.peek() == sOBJECT) {
      incrNodeObjectPath();
      setLastNode(0);
    }
    return LITERAL;
  }

  /**
   * Returns the current state. If the state of the top of the stack is a {@link #sARRAY_OBJECT},
   * it returns a {@link #sOBJECT} instead.
   */
  private int getCurrentState() {
    int state = states.peek();
    return state == sARRAY_OBJECT ? sOBJECT : state;
  }

%}

TRUE        = "true"
FALSE       = "false"
LONG        = -?[0-9][0-9]*({EXPONENT})?
DOUBLE      = -?[0-9][0-9]*\.[0-9]+({EXPONENT})?
EXPONENT    = [e|E][+|-]?[0-9]+
NULL        = "null"
ENDOFLINE   = \r|\n|\r\n
WHITESPACE  = {ENDOFLINE} | [ \t\f]

/*
   sSTRING: state for scanning a string
   sOBJECT: state for scanning an object
   sFIELD_STRING: state for scanning the field string in the object
   sFIELD: state for scanning the field
   sARRAY: scan arrays
   sARRAY_COMMA: this states allows for a multi-valued array
   sDATATYPE_STRING_VALUE: scan the value field of a datatype object
   sDATATYPE_LABEL: scan the label field of a datatype object

   sARRAY_OBJECT: state for scanning an object nested inside an array. This state indicates that we are in an object
   that was nested inside an array.

   While in state sFIELD, the last element of the states stack object
   is sOBJECT. The reason is that when the field string is scanned,
   the scanner goes to the sOBJECT thanks to `yybegin(states.peek());`.
   The scanner can enter in the sFIELD state only if scanning a new object,
   or if it matched a comma while scanning an object.

   While in state sARRAY, the last element of the states stack object
   is sARRAY_COMMA. The reason is that when a value pattern in sARRAY has been matched,
   the scanner goes to the sARRAY_COMMA, which only matching rules are either a comma,
   i.e., a new element in the array, or a closing bracket.
 */
%xstate sSTRING, sOBJECT, sFIELD, sFIELD_STRING, sARRAY, sARRAY_COMMA
%xstate sDATATYPE_STRING_VALUE, sDATATYPE_LABEL
%xstate sARRAY_OBJECT

%%

<YYINITIAL> {
  "{"                            { reset();
                                   states.push(sOBJECT);
                                   yybegin(sFIELD);
                                 }

  <sOBJECT, sFIELD, sARRAY, sARRAY_COMMA, sDATATYPE_LABEL, sDATATYPE_STRING_VALUE>
  {WHITESPACE}                   { /* ignore white space. */ }
}

<sOBJECT> {
  <sFIELD> // Share this rule with the sFIELD state so that empty objects are supported
  "}"                            { // A datatype object is already closed before,
                                   // in the state sDATATYPE_LABEL or sDATATYPE_STRING_VALUE
                                   if ((datatypeObject & DATATYPE_OBJ_ON) == DATATYPE_OBJ_ON && datatypeObject != DATATYPE_OBJ_OK) {
                                     if ((datatypeObject & DATATYPE_OBJ_LABEL) == DATATYPE_OBJ_LABEL) {
                                       throw new IllegalStateException(errorMessage("Uncomplete datatype object, missing _value_ field."));
                                     } else {
                                       throw new IllegalStateException(errorMessage("Uncomplete datatype object, missing _datatype_ field."));
                                     }
                                   } else if ((datatypeObject & DATATYPE_OBJ_ON) == DATATYPE_OBJ_OFF) {
                                     closeObject();
                                   } else if (datatypeObject == DATATYPE_OBJ_OK && states.peek() == sOBJECT) {
                                     // Decrement the node created in the state
                                     // sDATATYPE_LABEL or sDATATYPE_STRING_VALUE.
                                     // This only happens when in state sOBJECT, since it explicitly
                                     // adds a blank node (i.e., nestedObjects variable).
                                     // In the state sARRAY, the blank node is implicit.
                                     decrNodeObjectPath();
                                   }
                                   // when the states stack is empty, I am the root of the JSON tree
                                   yybegin(states.empty() ? YYINITIAL : this.getCurrentState());
                                   // Switch off the datatype object
                                   datatypeObject = DATATYPE_OBJ_OFF;
                                 }
  {NULL}                         { openLeafNode = true;
                                   incrNodeObjectPath();
                                   datatype = XSD_STRING;
                                   setLastNode(0);
                                   return NULL; }
  {FALSE}                        { openLeafNode = true;
                                   incrNodeObjectPath();
                                   datatype = XSD_BOOLEAN;
                                   setLastNode(0);
                                   return FALSE; }
  {TRUE}                         { openLeafNode = true;
                                   incrNodeObjectPath();
                                   datatype = XSD_BOOLEAN;
                                   setLastNode(0);
                                   return TRUE; }
  {LONG}                         { openLeafNode = true;
                                   incrNodeObjectPath();
                                   datatype = XSD_LONG;
                                   setLastNode(0);
                                   return processNumber(); }
  {DOUBLE}                       { openLeafNode = true;
                                   incrNodeObjectPath();
                                   datatype = XSD_DOUBLE;
                                   setLastNode(0);
                                   return processNumber(); }
  "["                            { incrNodeObjectPath();
                                   yybegin(sARRAY);
                                   states.push(sARRAY_COMMA);
                                   setLastNode(0); // initialise the first element of the array
                                 }
  "{"                            { initDatatypeObject();
                                   // Two incrementations, because the object introduce a "blank" node
                                   nestedObjects++;
                                   incrNodeObjectPath();
                                   setLastNode(0);
                                   incrNodeObjectPath();
                                   states.push(sOBJECT);
                                   yybegin(sFIELD);
                                 }
  \"                             { openLeafNode = true;
                                   incrNodeObjectPath();
                                   datatype = XSD_STRING;
                                   setLastNode(0);
                                   buffer.setLength(0);
                                   yybegin(sSTRING);
                                 }
  ","                            { if (openLeafNode) {
                                     openLeafNode = false;
                                     decrNodeObjectPath();
                                   }
                                   yybegin(sFIELD);
                                 }
  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found invalid character while scanning an object: [" + yytext() + "]")); }
}

<sFIELD> {
  \"                             { datatypeObject |= DATATYPE_OBJ_JUNK;
                                   if ((datatypeObject & DATATYPE_OBJ_ERROR) == DATATYPE_OBJ_ERROR) {
                                     throw new IllegalStateException(errorMessage("Wrong Datatype schema. Got unexpected text: " + yytext()));
                                   }
                                   datatype = JSON_FIELD;
                                   addToLastNode(1);
                                   buffer.setLength(0);
                                   yybegin(sFIELD_STRING);
                                 }
  // Datatype label
  \""_datatype_"\"{WHITESPACE}*":"{WHITESPACE}*\"
                                 {
                                   datatypeObject |= DATATYPE_OBJ_ON;
                                   if ((datatypeObject & DATATYPE_OBJ_LABEL) == DATATYPE_OBJ_LABEL) {
                                     throw new IllegalStateException(errorMessage("Wrong Datatype schema. The field _datatype_ appears several times."));
                                   }
                                   datatypeObject |= DATATYPE_OBJ_LABEL;
                                   if ((datatypeObject & DATATYPE_OBJ_ERROR) == DATATYPE_OBJ_ERROR) {
                                     throw new IllegalStateException(errorMessage("Wrong Datatype schema. Got unexpected elements in the datatype object."));
                                   }
                                   dtLabel.setLength(0);
                                   yybegin(sDATATYPE_LABEL);
                                 }
  // Datatype value: a string
  \""_value_"\"{WHITESPACE}*":"{WHITESPACE}*\"
                                 {
                                   datatypeObject |= DATATYPE_OBJ_ON;
                                   if ((datatypeObject & DATATYPE_OBJ_VALUE) == DATATYPE_OBJ_VALUE) {
                                     throw new IllegalStateException(errorMessage("Wrong Datatype schema. The field _value_ appears several times."));
                                   }
                                   datatypeObject |= DATATYPE_OBJ_VALUE;
                                   if ((datatypeObject & DATATYPE_OBJ_ERROR) == DATATYPE_OBJ_ERROR) {
                                     throw new IllegalStateException(errorMessage("Wrong Datatype schema. Got unexpected elements in the datatype object."));
                                   }
                                   buffer.setLength(0);
                                   yybegin(sDATATYPE_STRING_VALUE);
                                 }
  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found bad character while scanning an object: [" + yytext() + "]")); }
}

<sARRAY> {
  <sARRAY_COMMA> // Share this state with sARRAY_COMMA so that an empty array is supported
  "]"                            { decrNodeObjectPath();
                                   int state = states.pop();
                                   if (state != sARRAY_COMMA) {
                                     throw new IllegalStateException(errorMessage("Expected ']', got " + yychar()));
                                   }
                                   yybegin(this.getCurrentState());
                                 }
  "{"                            { initDatatypeObject();
                                   incrNodeObjectPath();
                                   states.push(sARRAY_OBJECT);
                                   yybegin(sFIELD);
                                 }
  "["                            { incrNodeObjectPath();
                                   states.push(sARRAY_COMMA);
                                   setLastNode(0); // initialise the first element of the array
                                 }
  {NULL}                         { datatype = XSD_STRING; yybegin(sARRAY_COMMA); return NULL; }
  {TRUE}                         { datatype = XSD_BOOLEAN; yybegin(sARRAY_COMMA); return TRUE; }
  {FALSE}                        { datatype = XSD_BOOLEAN; yybegin(sARRAY_COMMA); return FALSE; }
  {LONG}                         { datatype = XSD_LONG; yybegin(sARRAY_COMMA); return processNumber(); }
  {DOUBLE}                       { datatype = XSD_DOUBLE; yybegin(sARRAY_COMMA); return processNumber(); }
  \"                             { datatype = XSD_STRING;
                                   buffer.setLength(0);
                                   yybegin(sSTRING);
                                 }
  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found bad character while scanning an array: [" + yytext() + "]")); }
}

<sARRAY_COMMA> {
  ","                            { addToLastNode(1); yybegin(sARRAY); }
  // Error state
  .                              { throw new IllegalStateException(errorMessage("Found bad character while scanning an array: [" + yytext() + "]. Expecting either ',' or ']'.")); }
}

<sSTRING> {
  \"                             { yybegin(this.getCurrentState()); return LITERAL; }
  [^\"\\]+                       { buffer.append(yytext()); }
  \\\"                           { buffer.append('\"'); }
  \\.                            { buffer.append(yytext()); }
  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] { buffer.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16))); }
}

<sFIELD_STRING> {
  \"{WHITESPACE}*":"             { yybegin(sOBJECT); return LITERAL; }
  [^\"\\]+                       { buffer.append(yytext()); }
  \\\"                           { buffer.append('\"'); }
  \\.                            { buffer.append(yytext()); }
  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] { buffer.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16))); }
}

<sDATATYPE_LABEL> {
  \"                             { yybegin(sOBJECT);
                                   datatype = new char[dtLabel.length()];
                                   dtLabel.getChars(0, dtLabel.length(), datatype, 0);
                                   // the datatype value is here already
                                   if (datatypeObject == DATATYPE_OBJ_OK) {
                                     return closeDatatypeObject();
                                   }
                                 }
  [^\"\\]+                       { dtLabel.append(yytext()); }
  \\\"                           { dtLabel.append('\"'); }
  \\.                            { dtLabel.append(yytext()); }
  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] { dtLabel.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16))); }
}

<sDATATYPE_STRING_VALUE> {
  \"                             { yybegin(sOBJECT);
                                   // the datatype label is here already
                                   if (datatypeObject == DATATYPE_OBJ_OK) {
                                     return closeDatatypeObject();
                                   }
                                 }
  [^\"\\]+                       { buffer.append(yytext()); }
  \\\"                           { buffer.append('\"'); }
  \\.                            { buffer.append(yytext()); }
  \\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F] { buffer.append(Character.toChars(Integer.parseInt(new String(zzBufferL, zzStartRead+2, zzMarkedPos - zzStartRead - 2 ), 16))); }
}

/* Check that the states are empty */
<<EOF>>                          { if (!states.empty()) {
                                     throw new IllegalStateException(errorMessage("Check that all arrays/objects/strings are closed"));
                                   }
                                   return YYEOF;
                                 }

// catch all
.                                { throw new IllegalStateException(errorMessage("Found bad character: [" + yytext() + "]")); }
