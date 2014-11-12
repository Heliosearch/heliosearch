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
package com.sindicetech.siren.qparser.tree.parser;

import org.codehaus.jackson.JsonNode;

import com.sindicetech.siren.qparser.tree.ParseException;

/**
 * Parses an <code>attribute</code> property from a <code>node</code> object and returns a {@link String}.
 */
public class AttributePropertyParser extends JsonPropertyParser {

  public static final String ATTRIBUTE_PROPERTY = "attribute";

  AttributePropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return ATTRIBUTE_PROPERTY;
  }

  @Override
  String parse() throws ParseException {
    final JsonNode value = node.path(ATTRIBUTE_PROPERTY);
    if (value.isTextual()) {
      return value.asText();
    }
    throw new ParseException("Invalid property '" + ATTRIBUTE_PROPERTY + "': value is not a string");
  }

}
