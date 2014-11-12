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

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import com.sindicetech.siren.qparser.tree.ParseException;

/**
 * Parses a <code>range</code> property and returns an array of integers.
 */
public class RangePropertyParser extends JsonPropertyParser {

  public static final String RANGE_PROPERTY = "range";

  RangePropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return RANGE_PROPERTY;
  }

  @Override
  int[] parse() throws ParseException {
    final int[] range = new int[2];
    final JsonNode value = node.path(RANGE_PROPERTY);

    if (!(value.isArray() && (value.size() == 2))) {
      throw new ParseException("Invalid value for property '" + RANGE_PROPERTY + "'");
    }

    final Iterator<JsonNode> it = value.iterator();
    JsonNode e;
    for (int i = 0; i < 2; i++) {
      e = it.next();
      if (!e.isInt()) {
        throw new ParseException("Invalid property '" + RANGE_PROPERTY + "': range value is not an integer");
      }
      range[i] = e.asInt();
    }

    return range;
  }

}
