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

package com.sindicetech.siren.search.node;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

/**
 * A clause in a {@link NodeBooleanQuery}.
 *
 * <p>
 *
 * Code taken from {@link BooleanClause} and adapted for the Siren use case.
 **/
public class NodeBooleanClause implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The query whose matching documents are combined by the boolean query.
   */
  private NodeQuery query;

  private Occur occur;

  /**
   * Constructs a BooleanClause.
   */
  public NodeBooleanClause(final NodeQuery query, final Occur occur) {
    this.query = query;
    this.occur = occur;
  }

  public Occur getOccur() {
    return occur;
  }

  public void setOccur(final Occur occur) {
    this.occur = occur;
  }

  public NodeQuery getQuery() {
    return query;
  }

  public void setQuery(final NodeQuery query) {
    this.query = query;
  }

  public boolean isProhibited() {
    return Occur.MUST_NOT == occur;
  }

  public boolean isRequired() {
    return Occur.MUST == occur;
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(final Object o) {
    if (o == null || !(o instanceof NodeBooleanClause)) return false;
    final NodeBooleanClause other = (NodeBooleanClause) o;
    return this.query.equals(other.query) && this.occur.equals(other.occur);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return query.hashCode() ^ (Occur.MUST.equals(occur) ? 1 : 0) ^
           (Occur.MUST_NOT.equals(occur) ? 2 : 0);
  }

  @Override
  public String toString() {
    return occur.toString() + query.toString();
  }

  /** Specifies how clauses are to occur in matching documents. */
  public static enum Occur {

    /** Use this operator for clauses that <i>must</i> appear in the matching documents. */
    MUST     { @Override public String toString() { return "+"; } },

    /** Use this operator for clauses that <i>should</i> appear in the
     * matching documents. For a BooleanQuery with no <code>MUST</code>
     * clauses one or more <code>SHOULD</code> clauses must match a document
     * for the BooleanQuery to match.
     * @see BooleanQuery#setMinimumNumberShouldMatch
     */
    SHOULD   { @Override public String toString() { return "";  } },

    /** Use this operator for clauses that <i>must not</i> appear in the matching documents.
     * Note that it is not possible to search for queries that only consist
     * of a <code>MUST_NOT</code> clause. */
    MUST_NOT { @Override public String toString() { return "-"; } };

  }

}
