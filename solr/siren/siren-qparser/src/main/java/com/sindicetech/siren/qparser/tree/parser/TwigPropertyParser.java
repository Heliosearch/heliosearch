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
import com.sindicetech.siren.qparser.tree.nodes.ArrayQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.TwigQueryNode;

/**
 * Parses a <code>twig</code> property and returns a {@link TwigQueryNode}.
 */
public class TwigPropertyParser extends JsonPropertyParser {

  public static final String TWIG_PROPERTY = "twig";

  public TwigPropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  String getProperty() {
    return TWIG_PROPERTY;
  }

  @Override
  TwigQueryNode parse() throws ParseException {
    final TwigQueryNode twigNode = new TwigQueryNode();
    twigNode.setField(field);

    final JsonNode objectNode = node.path(this.getProperty());

    final RootPropertyParser rootParser = new RootPropertyParser(objectNode, field);
    rootParser.setOptional(true);
    if (rootParser.isPropertyDefined()) {
      twigNode.setRoot(rootParser.parse());
    }

    final LevelPropertyParser levelParser = new LevelPropertyParser(objectNode, field);
    levelParser.setOptional(true);
    if (levelParser.isPropertyDefined()) {
      twigNode.setTag(levelParser.getProperty(), levelParser.parse());
    }

    final RangePropertyParser rangeParser = new RangePropertyParser(objectNode, field);
    rangeParser.setOptional(true);
    if (rangeParser.isPropertyDefined()) {
      twigNode.setTag(rangeParser.getProperty(), rangeParser.parse());
    }

    final ChildPropertyParser childParser = new ChildPropertyParser(objectNode, field);
    childParser.setOptional(true);
    childParser.setDefaultModifier(this.getDefaultModifier());
    if (childParser.isPropertyDefined()) {
      final ArrayQueryNode arrayNode = childParser.parse();
      twigNode.add(arrayNode.getChildren());
    }

    final DescendantPropertyParser descendantParser = new DescendantPropertyParser(objectNode, field);
    descendantParser.setOptional(true);
    descendantParser.setDefaultModifier(this.getDefaultModifier());
    if (descendantParser.isPropertyDefined()) {
      final ArrayQueryNode arrayNode = descendantParser.parse();
      twigNode.add(arrayNode.getChildren());
    }

    return twigNode;
  }

}
