/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sindicetech.siren.solr.handler;

import com.sindicetech.siren.analysis.AbstractJsonTokenizer;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A stream-based JSON parser that outputs a stream of {@link com.sindicetech.siren.solr.handler.FieldEntry}.
 * This parser will flatten paths found in the JSON input, including nested arrays, mixed arrays, and nested objects in
 * arrays. It will also recognise the special nested datatype object from the SIREn syntax.<br/>
 *
 * TODO: Reconstruct the JSON input while parsing, in order to create a copy for SIREn indexing
 */
public class JsonReader {

  private final JsonParser parser;
  private final Deque<JsonToken> states = new ArrayDeque<>();
  private final Deque<String> path = new ArrayDeque<>();

  public JsonReader(JsonParser parser) throws IOException {
    this.parser = parser;

    JsonToken start = parser.nextToken();
    if (start != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("Illegal input: JSON document must start with an object.");
    }
    states.push(start);
  }

  public FieldEntry next() throws IOException {
    JsonToken token = null;
    FieldEntry entry = null;

    while ((token = parser.nextToken()) != null) {

      switch (token) {
        case START_OBJECT:
        case START_ARRAY:
          states.push(token);
          continue;

        case FIELD_NAME:
          String fieldname = parser.getCurrentName();
          if (fieldname.equals(AbstractJsonTokenizer.DATATYPE_LABEL)) {
            this.skipValue();
          }
          else if (!fieldname.equals(AbstractJsonTokenizer.DATATYPE_VALUES)) { // ignore datatype value field
            path.push(parser.getCurrentName());
          }
          continue;

        case VALUE_NULL:
        case VALUE_STRING:
          entry = new FieldEntry(path, parser.getText());
          if (states.peek() != JsonToken.START_ARRAY) { // if we are not in an array, we must pop the path
            path.poll();
          }
          return entry;

        case VALUE_TRUE:
        case VALUE_FALSE:
          entry = new FieldEntry(path, parser.getBooleanValue());
          if (states.peek() != JsonToken.START_ARRAY) { // if we are not in an array, we must pop the path
            path.poll();
          }
          return entry;

        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
          entry =  new FieldEntry(path, parser.getNumberValue());
          if (states.peek() != JsonToken.START_ARRAY) { // if we are not in an array, we must pop the path
            path.poll();
          }
          return entry;

        case END_ARRAY:
          if (states.poll() != JsonToken.START_ARRAY) {
            throw new IllegalArgumentException("Illegal input: JSON array not properly closed.");
          }
          if (states.peek() != JsonToken.START_ARRAY) { // if we are in a nested array, we must not pop the path
            path.poll();
          }
          continue;

        case END_OBJECT:
          if (states.poll() != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("Illegal input: JSON object not properly closed.");
          }
          if (states.peek() != JsonToken.START_ARRAY) { // if we are in a nested array, we must not pop the path
            path.poll();
          }
          continue;
      }

    }

    return null;
  }

  /**
   * Must be called when parser points to a {@link org.codehaus.jackson.JsonToken#FIELD_NAME} token. Skip the value,
   * i.e., a primitive, an array or an object, associated to the field.
   */
  private void skipValue() throws IOException {
    JsonToken token;

    assert parser.getCurrentToken() == JsonToken.FIELD_NAME;

    if ((token = parser.nextToken()) != null) {
      switch (token) {
        case START_ARRAY:
        case START_OBJECT:
          parser.skipChildren();

        default:
          return;
      }
    }
  }

}
