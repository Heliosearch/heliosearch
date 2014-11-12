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
package com.sindicetech.siren.qparser.tree.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import com.sindicetech.siren.qparser.tree.parser.LevelPropertyParser;

/**
 * This query node represents a descendant clause for a twig query and holds
 * the associated level of the descendant clause. It has the
 * same behaviour than {@link ModifierQueryNode}.
 */
public class DescendantQueryNode extends ModifierQueryNode {

  public DescendantQueryNode(final QueryNode query, final Modifier mod) {
    super(query, mod);
  }

  public void setLevel(final int level) {
    this.setTag(LevelPropertyParser.LEVEL_PROPERTY, level);
  }

  public int getLevel() {
    return (Integer) this.getTag(LevelPropertyParser.LEVEL_PROPERTY);
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "<descendant operation='" + this.getModifier().toString() + "' " +
    		"level='" + this.getLevel() + "'>\n"
        + this.getChild().toString() + "\n</descendant>";
  }

}
