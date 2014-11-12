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
package com.sindicetech.siren.qparser.keyword.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/**
 * A {@link ProtectedQueryNode} represents a term in which all special
 * characters are escaped.
 *
 * <p>
 *
 * For example, the colon in 'http://acme.org' is not interpreted as a twig
 * query operator, and the expression is not converted into a {@link TwigQueryNode}.
 */
public class ProtectedQueryNode extends FieldQueryNode {

  public ProtectedQueryNode(final CharSequence field, final CharSequence text,
                            final int begin, final int end) {
    super(field, text, begin, end);
  }

  @Override
  public CharSequence toQueryString(final EscapeQuerySyntax escaper) {
    return "'" + escaper + "'";
  }

  @Override
  public String toString() {
    return "<protected start='" + this.begin + "' end='" + this.end
        + "' field='" + this.field + "' term='" + this.text + "'/>";
  }

  @Override
  public ProtectedQueryNode cloneTree() throws CloneNotSupportedException {
    final ProtectedQueryNode clone = (ProtectedQueryNode) super.cloneTree();
    // nothing to do here
    return clone;
  }

}
