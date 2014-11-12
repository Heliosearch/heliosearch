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
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.codehaus.jackson.JsonNode;

import com.sindicetech.siren.qparser.tree.ParseException;
import com.sindicetech.siren.qparser.tree.nodes.ArrayQueryNode;
import com.sindicetech.siren.qparser.tree.nodes.DescendantQueryNode;

/**
 * Parses a <code>descendant</code> property and returns a {@link ArrayQueryNode}.
 */
public class DescendantPropertyParser extends AbstractClausePropertyParser {

  public static final String DESCENDANT_PROPERTY = "descendant";

  public DescendantPropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  @Override
  protected ModifierQueryNode newClauseQueryNode(final QueryNode query, final Modifier mod) {
    return new DescendantQueryNode(query, mod);
  }

  @Override
  protected ModifierQueryNode parseClauseOptions(final JsonNode element, final ModifierQueryNode node)
  throws ParseException {
    DescendantQueryNode descendant = (DescendantQueryNode) node;
    // parse level
    final LevelPropertyParser levelParser = new LevelPropertyParser(element, field);
    if (levelParser.isPropertyDefined()) {
      descendant.setLevel(levelParser.parse());
    }
    return descendant;
  }

  @Override
  String getProperty() {
    return DESCENDANT_PROPERTY;
  }

}
