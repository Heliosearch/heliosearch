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

import java.text.NumberFormat;

import org.apache.lucene.queryparser.flexible.standard.nodes.NumericQueryNode;

/**
 * This query node represents a field query that holds a numeric value.
 *
 * <p>
 *
 * This class is the same as {@link NumericQueryNode}, apart that it discards
 * the {@link NumberFormat} argument in {@link #toString()}.
 */
public class NodeNumericQueryNode
extends NumericQueryNode {

  public NodeNumericQueryNode(final CharSequence field,
                              final Number value) {
    super(field, value, null);
  }

  @Override
  public String toString() {
    return "<numeric field='" + this.getField() + "' number='"
    + this.getValue() + "'/>";
  }

}
