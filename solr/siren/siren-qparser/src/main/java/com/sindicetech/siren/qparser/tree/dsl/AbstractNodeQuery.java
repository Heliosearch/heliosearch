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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Abstract class that represents a node query object (either node or
 * twig query) of the JSON query syntax.
 */
public abstract class AbstractNodeQuery extends AbstractQuery {

  private boolean hasLevel = false;
  private int level;

  private boolean hasRange = false;
  private int lowerBound;
  private int upperBound;

  public AbstractNodeQuery(final ObjectMapper mapper) {
    super(mapper);
  }

  /**
   * Sets the node level constraint.
   *
   * @see {@link com.sindicetech.siren.search.node.NodeQuery#setLevelConstraint(int)}
   */
  public AbstractNodeQuery setLevel(final int level) {
    this.level = level;
    this.hasLevel = true;
    return this;
  }

  protected boolean hasLevel() {
    return hasLevel;
  }

  protected int getLevel() {
    return level;
  }

  /**
   * Sets the node range constraint.
   *
   * @see {@link com.sindicetech.siren.search.node.NodeQuery#setNodeConstraint(int, int)}
   */
  public AbstractNodeQuery setRange(final int lowerBound, final int upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.hasRange = true;
    return this;
  }

  protected boolean hasRange() {
    return hasRange;
  }

  protected int getLowerBound() {
    return lowerBound;
  }

  protected int getUpperBound() {
    return upperBound;
  }

  @Override
  public abstract Query toQuery(boolean proxy) throws QueryNodeException;

}
