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
import com.sindicetech.siren.qparser.tree.nodes.BooleanQueryNode;

/**
 * Parses a {@link #BOOLEAN_PROPERTY} property and returns a {@link BooleanQueryNode}.
 */
public class BooleanPropertyParser extends JsonPropertyParser {

  public static final String BOOLEAN_PROPERTY = "boolean";

  public BooleanPropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return BOOLEAN_PROPERTY;
  }

  @Override
  BooleanQueryNode parse() throws ParseException {
    final JsonNode value = node.path(this.getProperty());

    final BooleanQueryNode booleanNode = new BooleanQueryNode();

    final ClausePropertyParser clauseParser = new ClausePropertyParser(value, field);
    clauseParser.setDefaultModifier(this.getDefaultModifier());
    if (clauseParser.isPropertyDefined()) {
      booleanNode.add(clauseParser.parse().getChildren());
    }

    final SlopPropertyParser slopParser = new SlopPropertyParser(value, field);
    slopParser.setOptional(true);
    if (slopParser.isPropertyDefined()) {
      booleanNode.setTag(slopParser.getProperty(), slopParser.parse());
    }

    final InOrderPropertyParser orderedParser = new InOrderPropertyParser(value, field);
    orderedParser.setOptional(true);
    if (orderedParser.isPropertyDefined()) {
      booleanNode.setTag(orderedParser.getProperty(), orderedParser.parse());
    }

    final RangePropertyParser rangeParser = new RangePropertyParser(value, field);
    rangeParser.setOptional(true);
    if (rangeParser.isPropertyDefined()) {
      booleanNode.setTag(rangeParser.getProperty(), rangeParser.parse());
    }

    return booleanNode;
  }

}
