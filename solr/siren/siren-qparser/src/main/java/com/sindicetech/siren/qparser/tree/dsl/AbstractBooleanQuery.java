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

import org.codehaus.jackson.map.ObjectMapper;

import com.sindicetech.siren.qparser.tree.dsl.QueryClause.Occur;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that represents a boolean query object of the JSON query syntax.
 */
public abstract class AbstractBooleanQuery extends AbstractNodeQuery {

  protected final List<QueryClause> clauses;

  private boolean hasSlop = false;
  private int slop;

  private boolean hasInOrder = false;
  private boolean inOrder;

  AbstractBooleanQuery(final ObjectMapper mapper) {
    super(mapper);
    clauses = new ArrayList<QueryClause>();
  }

  @Override
  public AbstractBooleanQuery setBoost(final float boost) {
    throw new UnsupportedOperationException("Boost on AbstractBooleanQuery not supported");
  }

  /**
   * Has the slop parameter been set ?
   */
  boolean hasSlop() {
    return hasSlop;
  }

  /**
   * Retrieve the slop parameter
   */
  int getSlop() {
    return slop;
  }

  /**
   * Sets the slop for this query.
   */
  public AbstractBooleanQuery setSlop(final int slop) {
    this.slop = slop;
    this.hasSlop = true;
    return this;
  }

  /**
   * Has the inOrder parameter been set ?
   */
  boolean hasInOrder() {
    return hasInOrder;
  }

  /**
   * Retrieve the inOrder parameter
   */
  boolean getInOrder() {
    return inOrder;
  }

  /**
   * Sets the inOrder for this query.
   */
  public AbstractBooleanQuery setInOrder(final boolean inOrder) {
    this.inOrder = inOrder;
    this.hasInOrder = true;
    return this;
  }

  /**
   * Adds a boolean clause with a
   * {@link org.apache.lucene.search.BooleanClause.Occur#MUST} operator.
   * <p>
   * Use this method for clauses that must appear in the matching documents.
   *
   * @see {@link org.apache.lucene.search.BooleanClause.Occur#MUST}
   */
  public AbstractBooleanQuery with(final AbstractNodeQuery node) {
    clauses.add(new BasicQueryClause(node, Occur.MUST));
    return this;
  }

  /**
   * Adds a boolean clause with a
   * {@link org.apache.lucene.search.BooleanClause.Occur#MUST_NOT} operator.
   * <p>
   * Use this method for clauses that must not appear in the matching documents.
   *
   * @see {@link org.apache.lucene.search.BooleanClause.Occur#MUST_NOT}
   */
  public AbstractBooleanQuery without(final AbstractNodeQuery node) {
    clauses.add(new BasicQueryClause(node, Occur.MUST_NOT));
    return this;
  }

  /**
   * Adds a boolean clause with a
   * {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD} operator.
   * <p>
   * Use this method for clauses that should appear in the matching documents.
   *
   * @see {@link org.apache.lucene.search.BooleanClause.Occur#SHOULD}
   */
  public AbstractBooleanQuery optional(final AbstractNodeQuery node) {
    clauses.add(new BasicQueryClause(node, Occur.SHOULD));
    return this;
  }

}
