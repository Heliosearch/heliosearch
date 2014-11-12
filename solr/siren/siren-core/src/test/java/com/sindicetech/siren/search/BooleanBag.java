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

package com.sindicetech.siren.search;

import java.util.ArrayList;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;
import com.sindicetech.siren.search.node.NodeBooleanClause;
import com.sindicetech.siren.search.node.NodeQuery;

/**
 * This class represents a combination of {@link Query}s with
 * a specific Occur.
 */
public class BooleanBag {

  private final boolean isMust;
  private final boolean isNot;
  private final boolean isShould;
  private final ArrayList<Query> queries = new ArrayList<Query>();

  private BooleanBag(Query[] queries, boolean isMust, boolean isNot, boolean isShould) {
    this.isShould = isShould;
    this.isMust = isMust;
    this.isNot = isNot;
    for (Query q : queries) {
      this.queries.add(q);
    }
  }

  public static BooleanBag must(Query...queries) {
    return new BooleanBag(queries, true, false, false);
  }

  public static BooleanBag should(Query...queries) {
    return new BooleanBag(queries, false, false, true);
  }

  public static BooleanBag not(Query...queries) {
    return new BooleanBag(queries, false, true, false);
  }

  public NodeBooleanClause[] toNodeBooleanClauses() {
    final NodeBooleanClause[] clauses = new NodeBooleanClause[queries.size()];

    for (int i = 0; i < clauses.length; i++) {
      final Query q = queries.get(i);
      if (q instanceof NodeQuery) {
        final NodeBooleanClause.Occur occur;
        if (isMust) {
          occur = NodeBooleanClause.Occur.MUST;
        } else if (isNot) {
          occur = NodeBooleanClause.Occur.MUST_NOT;
        } else if (isShould) {
          occur = NodeBooleanClause.Occur.SHOULD;
        } else {
          // Shouldn't happen
          throw new IllegalArgumentException("No occurrence could be built!");
        }
        clauses[i] = new NodeBooleanClause((NodeQuery) q, occur);
      } else {
        throw new IllegalArgumentException("Building NodeBooleanClauses, " +
            "expecting only NodeQuery, but got: " + q.getClass().getName());
      }
    }
    return clauses;
  }

  public BooleanClause[] toBooleanClauses() {
    final BooleanClause[] clauses = new BooleanClause[queries.size()];

    for (int i = 0; i < clauses.length; i++) {
      Query q = queries.get(i);
      if (q instanceof NodeQuery) {
        q = new LuceneProxyNodeQuery((NodeQuery) q);
      }
      final BooleanClause.Occur occur;
      if (isMust) {
        occur = BooleanClause.Occur.MUST;
      } else if (isNot) {
        occur = BooleanClause.Occur.MUST_NOT;
      } else if (isShould) {
        occur = BooleanClause.Occur.SHOULD;
      } else {
        // Shouldn't happen
        throw new IllegalArgumentException("No occurrence could be built!");
      }
      clauses[i] = new BooleanClause(q, occur);
    }
    return clauses;
  }

}
