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
package com.sindicetech.siren.util;

import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.analysis.ExtendedJsonTokenizer;
import com.sindicetech.siren.util.ArrayUtils;
import com.sindicetech.siren.util.JSONDatatype;
import com.sindicetech.siren.util.XSDDatatype;

import java.util.*;

import static com.sindicetech.siren.analysis.ExtendedJsonTokenizer.*;

public class JsonGenerator {

  public final Random          rand;
  public int                   valueType;

  // used for generating a random json document
  private final StringBuilder  sb           = new StringBuilder();
  private final Stack<Integer> states       = new Stack<Integer>();
  private static final int     ARRAY        = 0;
  private static final int     OBJECT_ATT   = 1;
  private static final int     OBJECT_VAL   = 2;
  /** This state is for specifically handling a nested object in an array */
  private static final int     ARRAY_OBJECT = 3;
  public final List<String>    images       = new ArrayList<String>();
  public final List<IntsRef>   nodes        = new ArrayList<IntsRef>();
  public final List<Integer>   incr         = new ArrayList<Integer>();
  public final List<String>    types        = new ArrayList<String>();
  public final List<String>    datatypes    = new ArrayList<String>();
  private final IntsRef        curNodePath  = new IntsRef(1024);
  public boolean               shouldFail   = false;
  private final int            MAX_DEPTH    = 50;
  private int                  nestedObjs   = 0;

  public JsonGenerator(final Random rand) {
    this.rand = rand;
  }

  /**
   * Create a random Json document with random values
   */
  public String getRandomJson(int nbNodes) {
    // init
    sb.setLength(0);
    sb.append("{");
    states.clear();
    states.add(OBJECT_ATT);
    images.clear();
    nodes.clear();
    incr.clear();
    datatypes.clear();
    types.clear();
    curNodePath.length = 1;
    curNodePath.offset = 0;
    Arrays.fill(curNodePath.ints, -1);
    shouldFail = false;
    nestedObjs = 0;

    // <= so that when nbNodes == 1, the json is still valid
    /*
     * the generated json might be uncomplete, if states is not empty, and
     * the maximum number of nodes has been reached.
     */
    for (final int i = 0; i <= nbNodes && !states.empty(); nbNodes++) {
      sb.append(this.getWhitespace()).append(this.getNextNode()).append(this.getWhitespace());
    }
    shouldFail = shouldFail ? true : !states.empty();
    return sb.toString();
  }

  /**
   * Return the next element of the json document
   */
  private String getNextNode() {
    final int popState;

    switch (states.peek()) {
      case ARRAY:
        switch (rand.nextInt(9)) {
          case 0: // String case
            final String val = "stepha" + this.getWhitespace() + "n" + this.getWhitespace() + "e";
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            images.add(val);
            types.add(ExtendedJsonTokenizer.getTokenTypes()[LITERAL]);
            incr.add(1);
            datatypes.add(XSDDatatype.XSD_STRING);
            return "\"" + val + "\"" + this.getWhitespace() + ",";
          case 1: // DOUBLE case
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            images.add("34.560e-9");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[NUMBER]);
            incr.add(1);
            datatypes.add(XSDDatatype.XSD_DOUBLE);
            return "34.560e-9" + this.getWhitespace() + ",";
          case 2: // LONG case
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            images.add("34560e-9");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[NUMBER]);
            incr.add(1);
            datatypes.add(XSDDatatype.XSD_LONG);
            return "34560e-9" + this.getWhitespace() + ",";
          case 3: // true case
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            images.add("true");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[TRUE]);
            incr.add(1);
            datatypes.add(XSDDatatype.XSD_BOOLEAN);
            return "true" + this.getWhitespace() + ",";
          case 4: // false case
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            images.add("false");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[FALSE]);
            incr.add(1);
            datatypes.add(XSDDatatype.XSD_BOOLEAN);
            return "false" + this.getWhitespace() + ",";
          case 5: // null case
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            images.add("null");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[NULL]);
            incr.add(1);
            datatypes.add(XSDDatatype.XSD_STRING);
            return "null" + this.getWhitespace() + ",";
          case 6: // nested array case
            if (states.size() <= MAX_DEPTH) {
              this.addToLastNode(1);
              this.incrNodeObjectPath();
              states.add(ARRAY);
              return "[";
            }
            return "";
          case 7: // nested object case
            if (states.size() <= MAX_DEPTH) {
              this.addToLastNode(1);
              this.incrNodeObjectPath();
              states.add(ARRAY_OBJECT);
              return "{";
            }
            return "";
          case 8: // closing array case
            this.decrNodeObjectPath();
            popState = states.pop();
            if (popState != ARRAY) {
              shouldFail = true;
            }
            // Remove previous comma, this is not allowed
            final int comma = sb.lastIndexOf(",");
            if (comma != -1 && sb.substring(comma + 1).matches("\\s*")) {
              sb.deleteCharAt(comma);
            }
            return "],";
        }
      case ARRAY_OBJECT:
      case OBJECT_ATT:
        switch (rand.nextInt(3)) {
          case 0: // new object field
            types.add(ExtendedJsonTokenizer.getTokenTypes()[LITERAL]);
            images.add("ste ph ane");
            incr.add(1);
            this.addToLastNode(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            datatypes.add(JSONDatatype.JSON_FIELD);

            states.push(OBJECT_VAL);
            return "\"ste ph ane\"" + this.getWhitespace() + ":";
          case 1: // close object
            if (states.peek() == OBJECT_ATT && nestedObjs > 0) {
              this.decrNodeObjectPath();
              nestedObjs--;
            }
            this.decrNodeObjectPath();
            popState = states.pop();
            if (popState != OBJECT_ATT && popState != ARRAY_OBJECT) {
              shouldFail = true;
            }
            // Remove previous comma, this is not allowed
            final int comma = sb.lastIndexOf(",");
            if (comma != -1 && sb.substring(comma + 1).matches("\\s*")) {
              sb.deleteCharAt(comma);
            }
            return states.empty() ? "}" : "},";
          case 2: // Datatype
            if (getLastNode() >= 0) {
              // this nested object cannot be a datatype object because other things have been added to it
              return "";
            }
            final String field;
            if (states.isEmpty()) {
              // datatype object at the root are not possible
              shouldFail = true;
              field = "{";
            } else if (states.peek() == OBJECT_ATT) {
              // field name
              this.addToLastNode(1);
              field = "\"field\":{";
              types.add(ExtendedJsonTokenizer.getTokenTypes()[LITERAL]);
              images.add("field");
              incr.add(1);
              nodes.add(IntsRef.deepCopyOf(curNodePath));
              datatypes.add(JSONDatatype.JSON_FIELD);
              // value
              this.incrNodeObjectPath();
              this.setLastNode(0);
            } else if (states.peek() == ARRAY) {
              this.addToLastNode(1);
              field = "{";
            } else if (states.peek() == ARRAY_OBJECT) {
              this.decrNodeObjectPath();
              field = "";
            } else {
              // should not happen
              throw new IllegalStateException("Received unknown state=" + states.peek());
            }

            types.add(ExtendedJsonTokenizer.getTokenTypes()[LITERAL]);
            images.add("Luke Skywalker");
            incr.add(1);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            datatypes.add("jedi");
            // close datatype object
            if (states.peek() == ARRAY_OBJECT) {
              popState = states.pop();
            } else {
              this.decrNodeObjectPath();
            }
            return field + this.getWhitespace() +
                      "\"" + ExtendedJsonTokenizer.DATATYPE_LABEL + "\":" + this.getWhitespace() + "\"jedi\"," +
                      "\"" + ExtendedJsonTokenizer.DATATYPE_VALUES + "\":" + this.getWhitespace() + "\"Luke Skywalker\"" +
                    this.getWhitespace() + "},";
        }
      case OBJECT_VAL:
        switch (rand.nextInt(8)) {
          case 0: // String
            return this.doValString("stepha" + this.getWhitespace() + "n" + this.getWhitespace() + "e");
          case 1: // DOUBLE case
            images.add("34.560e-9");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[NUMBER]);
            incr.add(1);
            this.incrNodeObjectPath();
            this.setLastNode(0);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            this.decrNodeObjectPath();
            datatypes.add(XSDDatatype.XSD_DOUBLE);

            states.pop(); // remove OBJECT_VAL state
            return "34.560e-9" + this.getWhitespace() + ",";
          case 2: // LONG case
            images.add("34560e-9");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[NUMBER]);
            incr.add(1);
            this.incrNodeObjectPath();
            this.setLastNode(0);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            this.decrNodeObjectPath();
            datatypes.add(XSDDatatype.XSD_LONG);

            states.pop(); // remove OBJECT_VAL state
            return "34560e-9" + this.getWhitespace() + ",";
          case 3: // True
            images.add("true");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[TRUE]);
            incr.add(1);
            this.incrNodeObjectPath();
            this.setLastNode(0);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            this.decrNodeObjectPath();
            datatypes.add(XSDDatatype.XSD_BOOLEAN);

            states.pop(); // remove OBJECT_VAL state
            return "true" + this.getWhitespace() + ",";
          case 4: // False
            images.add("false");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[FALSE]);
            incr.add(1);
            this.incrNodeObjectPath();
            this.setLastNode(0);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            this.decrNodeObjectPath();
            datatypes.add(XSDDatatype.XSD_BOOLEAN);

            states.pop(); // remove OBJECT_VAL state
            return "false" + this.getWhitespace() + ",";
          case 5: // NULL
            images.add("null");
            types.add(ExtendedJsonTokenizer.getTokenTypes()[NULL]);
            incr.add(1);
            this.incrNodeObjectPath();
            this.setLastNode(0);
            nodes.add(IntsRef.deepCopyOf(curNodePath));
            this.decrNodeObjectPath();
            datatypes.add(XSDDatatype.XSD_STRING);

            states.pop(); // remove OBJECT_VAL state
            return "null" + this.getWhitespace() + ",";
          case 6: // New array
            if (states.size() <= MAX_DEPTH) {
              states.pop(); // remove OBJECT_VAL state
              this.incrNodeObjectPath();
              states.add(ARRAY);
              return "[";
            }
            return this.doValString("");
          case 7: // new Object
            if (states.size() <= MAX_DEPTH) {
              states.pop(); // remove OBJECT_VAL state
              // Two incrementations, because the object introduce a "blank" node
              nestedObjs++;
              this.incrNodeObjectPath();
              this.setLastNode(0);
              this.incrNodeObjectPath();
              states.add(OBJECT_ATT);
              return "{";
            }
            return this.doValString("");
        }
      default:
        throw new IllegalStateException("Got unknown lexical state: " + states.peek());
    }
  }

  /**
   * Return a sequence of whitespace characters
   */
  private String getWhitespace() {
    final int nWS = rand.nextInt(5);
    String ws = "";

    for (int i = 0; i < nWS; i++) {
      switch (rand.nextInt(6)) {
        case 0:
          ws += " ";
          break;
        case 1:
          ws += "\t";
         break;
        case 2:
          ws += "\f";
          break;
        case 3:
          ws += "\r";
          break;
        case 4:
          ws += "\n";
          break;
        case 5:
          ws += "\r\n";
          break;
        default:
          break;
      }
    }
    return ws;
  }

  /**
   * Add an object/array to the current node path
   */
  private void incrNodeObjectPath() {
    ArrayUtils.growAndCopy(curNodePath, curNodePath.length + 1);
    curNodePath.length++;
    // initialise node
    this.setLastNode(-1);
  }

  /**
   * Remove an object/array from the node path
   */
  private void decrNodeObjectPath() {
    curNodePath.length--;
  }

  /** Update the path of the current values of the current object node */
  private void setLastNode(final int val) {
    curNodePath.ints[curNodePath.length - 1] = val;
  }

  /** Update the path of the current values of the current object node */
  private void addToLastNode(final int val) {
    curNodePath.ints[curNodePath.length - 1] += val;
  }

  /**
   * Returns the position of the last node
   */
  private int getLastNode() {
    return curNodePath.ints[curNodePath.length - 1];
  }

  /**
   * Add a string value to an object entry
   */
  private String doValString(final String val) {
    images.add(val);
    types.add(ExtendedJsonTokenizer.getTokenTypes()[LITERAL]);
    incr.add(1);
    this.incrNodeObjectPath();
    this.setLastNode(0);
    nodes.add(IntsRef.deepCopyOf(curNodePath));
    this.decrNodeObjectPath();
    datatypes.add(XSDDatatype.XSD_STRING);

    states.pop(); // remove OBJECT_VAL state
    return "\"" + val + "\"" + this.getWhitespace() + ",";
  }

  /**
   * Returns a random value type
   */
  public String getRandomValue() {
    valueType = rand.nextInt(5);

    switch (valueType) {
      case FALSE:
        return "false";
      case LITERAL:
        return "\"stephane\"";
      case NULL:
        return "null";
      case NUMBER:
        return "324.90E-02";
      case TRUE:
        return "true";
      default:
        throw new IllegalArgumentException("No value for index=" + valueType);
    }
  }

}
