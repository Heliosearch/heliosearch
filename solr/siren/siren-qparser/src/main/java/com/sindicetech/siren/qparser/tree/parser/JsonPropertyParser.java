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

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.codehaus.jackson.JsonNode;

import com.sindicetech.siren.qparser.tree.ParseException;

/**
 * Abstraction over the JSON property parsers.
 */
abstract class JsonPropertyParser {

  final CharSequence field;

  final JsonNode node;

  boolean optional = false;

  private ModifierQueryNode.Modifier defaultModifier;

  JsonPropertyParser(final JsonNode node, final CharSequence field) {
    this.node = node;
    this.field = field;
  }

  /**
   * Set the specified property as optional
   */
  void setOptional(final boolean optional) {
    this.optional = optional;
  }

  /**
   * Return true if the specified property is optional
   */
  boolean isOptional() {
    return optional;
  }

  /**
   * Set the default modifier to use
   */
  void setDefaultModifier(final ModifierQueryNode.Modifier modifier) {
    this.defaultModifier = modifier;
  }

  ModifierQueryNode.Modifier getDefaultModifier() {
    if (this.defaultModifier == null) {
      throw new IllegalArgumentException("Default modifier is not set");
    }
    return this.defaultModifier;
  }

  /**
   * Check if the {@link JsonNode} is a JSON Object node and contains value for
   * specified property. If this is the case, returns true; otherwise returns
   * false.
   * <p>
   * If the property is not defined and is not optional, throw a
   * {@link ParseException}.
   */
  boolean isPropertyDefined() throws ParseException {
    if (node.has(this.getProperty())) {
      return true;
    }
    else {
      if (this.optional) {
        return false;
      }
      throw new ParseException("Missing property '" + this.getProperty() + "'");
    }
  }

  /**
   * Return the property associated to this parser
   */
  abstract String getProperty();

  abstract Object parse() throws ParseException;

}
