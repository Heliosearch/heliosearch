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

package com.sindicetech.siren.search.spans;

import com.sindicetech.siren.search.node.Datatyped;

/**
 * A {@link com.sindicetech.siren.search.node.Datatyped} {@link SpanQuery}, such as
 * {@link com.sindicetech.siren.search.spans.TermSpanQuery}.
 */
public abstract class DatatypedSpanQuery extends SpanQuery implements Datatyped {

  protected String datatype = null;

  /**
   * Set the datatype associated with this query.
   */
  public void setDatatype(final String datatype) {
    this.datatype = datatype;
  }

  /**
   * Wraps the #toString output with the datatype query syntax
   */
  protected StringBuilder wrapToStringWithDatatype(final StringBuilder builder) {
    if (datatype != null) {
      builder.insert(0, datatype + "(");
      builder.append(')');
    }
    return builder;
  }

}
