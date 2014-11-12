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
import org.codehaus.jackson.node.ObjectNode;

import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;

/**
 * Abstract class that represents a query object of the JSON query
 * syntax.
 */
public abstract class AbstractQuery {

  protected final ObjectMapper mapper;

  private boolean hasBoost = false;
  private float boost;

  public AbstractQuery(final ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /**
   * Convert the constructed query into a {@link Query}.
   *
   * @param proxy Should the query be wrapped into a {@link LuceneProxyNodeQuery} ?
   */
  public abstract Query toQuery(boolean proxy) throws QueryNodeException;

  /**
   * Return a JSON representation of the constructed query.
   * <p>
   * The JSON representation is compatible with the {@link com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser}.
   *
   * @throws IllegalArgumentException If the created query object cannot be
   * converted to JSON.
   */
  @Override
  public String toString() {
    final ObjectNode node = this.toJson();
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
    }
    catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Build a JSON object {link ObjectNode} which maps the content of the query
   * object to the JSON query syntax.
   */
  public abstract ObjectNode toJson();

  /**
   * Has the boost parameter been set ?
   */
  boolean hasBoost() {
    return hasBoost;
  }

  /**
   * Retrieve the boost parameter
   */
  float getBoost() {
    return boost;
  }

  /**
   * Sets the boost for this query.
   *
   * @see {@link Query#setBoost(float)}
   */
  public AbstractQuery setBoost(final float boost) {
    this.boost = boost;
    this.hasBoost = true;
    return this;
  }

}
