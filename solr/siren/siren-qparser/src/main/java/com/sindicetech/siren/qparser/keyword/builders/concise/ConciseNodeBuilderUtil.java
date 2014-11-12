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
package com.sindicetech.siren.qparser.keyword.builders.concise;

import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;

import com.sindicetech.siren.analysis.filter.PathEncodingFilter;
import com.sindicetech.siren.qparser.keyword.builders.NodeFuzzyQueryNodeBuilder;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * Set of utility methods for building a {@link NodeQuery} for the concise model.
 */
class ConciseNodeBuilderUtil extends NodeFuzzyQueryNodeBuilder {

  /**
   * Prepend the attribute to the encoded value of the
   * {@link org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode}. A reusable
   * {@link java.lang.StringBuilder} is required for the operation.
   */
  static void prepend(final StringBuilder builder, final String attribute, final TextableQueryNode fieldNode) {
    // Prepend the attribute to the term
    String value = prepend(builder, attribute, fieldNode.getText());
    // Update the encoded value of the TextableQueryNode
    fieldNode.setText(value);
  }

  /**
   * Prepend the attribute to the value. A reusable
   * {@link java.lang.StringBuilder} is required for the operation.
   */
  static String prepend(final StringBuilder builder, final CharSequence attribute, final CharSequence value) {
    // Prepend the attribute to the term
    builder.setLength(0);
    builder.append(attribute);
    builder.append(PathEncodingFilter.PATH_DELIMITER);
    builder.append(value);
    return builder.toString();
  }

}
