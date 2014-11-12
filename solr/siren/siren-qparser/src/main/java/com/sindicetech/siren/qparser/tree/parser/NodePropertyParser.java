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
import com.sindicetech.siren.qparser.tree.nodes.NodeQueryNode;

/**
 * Parses a <code>node</code> property and returns a {@link NodeQueryNode}.
 * <p>
 * A <code>node</code> property must have an object composed of:
 * <ul>
 *   <li>a mandatory <code>query</code> property</li>,
 *   <li>an optional <code>attribute</code> property</li>,
 *   <li>an optional <code>level</code> property</li>,
 *   <li>an optional <code>range</code> property</li>,
 * </ul>
 */
public class NodePropertyParser extends JsonPropertyParser {

  public static final String NODE_PROPERTY = "node";

  public NodePropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return NODE_PROPERTY;
  }

  @Override
  NodeQueryNode parse() throws ParseException {
    final NodeQueryNode queryNode = new NodeQueryNode();
    queryNode.setField(field);

    final JsonNode objectNode = node.path(this.getProperty());

    final QueryPropertyParser queryParser = new QueryPropertyParser(objectNode, field);
    if (queryParser.isPropertyDefined()) {
      queryNode.setValue(queryParser.parse());
    }

    final AttributePropertyParser attributeParser = new AttributePropertyParser(objectNode, field);
    attributeParser.setOptional(true);
    if (attributeParser.isPropertyDefined()) {
       queryNode.setAttribute(attributeParser.parse());
    }

    final LevelPropertyParser levelParser = new LevelPropertyParser(objectNode, field);
    levelParser.setOptional(true);
    if (levelParser.isPropertyDefined()) {
      queryNode.setTag(levelParser.getProperty(), levelParser.parse());
    }

    final RangePropertyParser rangeParser = new RangePropertyParser(objectNode, field);
    rangeParser.setOptional(true);
    if (rangeParser.isPropertyDefined()) {
      queryNode.setTag(rangeParser.getProperty(), rangeParser.parse());
    }

    return queryNode;
  }

}
