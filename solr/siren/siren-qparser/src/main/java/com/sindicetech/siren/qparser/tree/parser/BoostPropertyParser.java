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
 * Parses a <code>boost</code> property and returns a {@link Float}.
 */
public class BoostPropertyParser extends JsonPropertyParser {

  public static final String BOOST_PROPERTY = "boost";

  BoostPropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return BOOST_PROPERTY;
  }

  @Override
  Float parse() throws ParseException {
    final JsonNode value = node.path(BOOST_PROPERTY);
    if (value.isFloatingPointNumber()) {
      return (float) value.asDouble();
    }
    throw new ParseException("Invalid property '" + BOOST_PROPERTY + "': value is not a float");
  }

}
