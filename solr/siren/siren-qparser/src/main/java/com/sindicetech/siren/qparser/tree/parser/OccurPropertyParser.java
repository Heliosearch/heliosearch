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

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;
import org.apache.lucene.search.BooleanClause.Occur;
import org.codehaus.jackson.JsonNode;

import com.sindicetech.siren.qparser.tree.ParseException;

/**
 * Parses a <code>occur</code> property and returns a {@link Modifier}.
 */
public class OccurPropertyParser extends JsonPropertyParser {

  public static final String OCCUR_PROPERTY = "occur";

  public OccurPropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return OCCUR_PROPERTY;
  }

  @Override
  Modifier parse() throws ParseException {
    final JsonNode value = node.path(OCCUR_PROPERTY);

    if (!value.isTextual()) {
      throw new ParseException("Invalid property'" + OCCUR_PROPERTY + "': value is not textual");
    }

    try {
      final Occur occur = Occur.valueOf(value.asText());
      switch (occur) {
        case MUST:
          return Modifier.MOD_REQ;

        case SHOULD:
          return Modifier.MOD_NONE;

        case MUST_NOT:
          return Modifier.MOD_NOT;

        default:
          throw new ParseException("Invalid value '" + value.asText() + "' for property '" + OCCUR_PROPERTY + "'");
      }
    }
    catch (final IllegalArgumentException e) {
      throw new ParseException("Invalid value '" + value.asText() + "' for property '" + OCCUR_PROPERTY + "'", e);
    }
  }

}
