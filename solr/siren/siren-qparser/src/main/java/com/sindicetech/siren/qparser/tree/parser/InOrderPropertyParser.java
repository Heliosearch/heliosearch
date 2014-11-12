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
 * Parses a <code>inOrder</code> property and returns an {@link Integer}.
 */
public class InOrderPropertyParser extends JsonPropertyParser {

  public static final String IN_ORDER_PROPERTY = "inOrder";

  InOrderPropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return IN_ORDER_PROPERTY;
  }

  @Override
  Integer parse() throws ParseException {
    final JsonNode value = node.path(IN_ORDER_PROPERTY);
    if (value.isBoolean()) {
      return value.asBoolean() ? 1 : 0;
    }
    throw new ParseException("Invalid property '" + IN_ORDER_PROPERTY + "': value is not an boolean");
  }

}
