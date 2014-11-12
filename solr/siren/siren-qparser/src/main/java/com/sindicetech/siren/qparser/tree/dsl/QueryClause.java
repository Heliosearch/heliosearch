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
package com.sindicetech.siren.qparser.tree.dsl;

import org.apache.lucene.search.BooleanClause;

import com.sindicetech.siren.search.node.NodeBooleanClause;

/**
 * Abstract class that represents a clause object of the JSON query syntax.
 */
abstract class QueryClause {

  private final Occur occur;

  private final AbstractNodeQuery query;

  QueryClause(final AbstractNodeQuery query, final Occur occur) {
    this.query = query;
    this.occur = occur;
  }

  AbstractNodeQuery getQuery() {
    return query;
  }

  Occur getOccur() {
    return occur;
  }

  BooleanClause.Occur getBooleanOccur() {
    return toBooleanOccur(occur);
  }

  NodeBooleanClause.Occur getNodeBooleanOccur() {
    return toNodeBooleanOccur(occur);
  }

  enum Occur { MUST, MUST_NOT, SHOULD }

  static NodeBooleanClause.Occur toNodeBooleanOccur(final Occur occur) {
    switch (occur) {
      case MUST:
        return NodeBooleanClause.Occur.MUST;

      case MUST_NOT:
        return NodeBooleanClause.Occur.MUST_NOT;

      case SHOULD:
        return NodeBooleanClause.Occur.SHOULD;

      default:
        throw new IllegalArgumentException("Unknown occur received");
    }
  }

  static BooleanClause.Occur toBooleanOccur(final Occur occur) {
    switch (occur) {
      case MUST:
        return BooleanClause.Occur.MUST;

      case MUST_NOT:
        return BooleanClause.Occur.MUST_NOT;

      case SHOULD:
        return BooleanClause.Occur.SHOULD;

      default:
        throw new IllegalArgumentException("Unknown occur received");
    }
  }

}
