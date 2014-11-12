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

/**
 * This query node represents a child clause for a twig query. It has the same
 * behaviour than {@link ModifierQueryNode}.
 */
public class ChildQueryNode extends ModifierQueryNode {

  public ChildQueryNode(final QueryNode query, final Modifier mod) {
    super(query, mod);
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escapeSyntaxParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "<child operation='" + this.getModifier().toString() + "'>" + "\n"
        + this.getChild().toString() + "\n</child>";
  }

}
