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
import com.sindicetech.siren.qparser.tree.nodes.VariableQueryNode;

/**
 * Parses a <code>variable</code> property and returns a {@link VariableQueryNode}.
 * 
 */
public class VariablePropertyParser extends JsonPropertyParser {

  public static final String VARIABLE_PROPERTY = "variable";

  public VariablePropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return VARIABLE_PROPERTY;
  }

  @Override
  VariableQueryNode parse() throws ParseException {
    final VariableQueryNode queryNode = new VariableQueryNode();
    final JsonNode value = node.path(this.getProperty());

    if (value.getFields().hasNext()) {
        throw new ParseException("Node variable must contain an empty object as value.");
    }

    return queryNode;
  }

}
