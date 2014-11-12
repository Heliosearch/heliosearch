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
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.sindicetech.siren.qparser.keyword.ExtendedKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.parser.BoostPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.LevelPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.NodePropertyParser;
import com.sindicetech.siren.qparser.tree.parser.QueryPropertyParser;
import com.sindicetech.siren.qparser.tree.parser.RangePropertyParser;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;

/**
 * Class that represents a node query object of the JSON query syntax.
 */
public class NodeQuery extends AbstractNodeQuery {

  protected final ExtendedKeywordQueryParser parser;

  protected String booleanExpression;

  NodeQuery(final ObjectMapper mapper, final ExtendedKeywordQueryParser parser) {
    super(mapper);
    this.parser = parser;
  }

  void setBooleanExpression(final String booleanExpression) {
    this.booleanExpression = booleanExpression;
  }

  @Override
  public NodeQuery setLevel(final int level) {
    return (NodeQuery) super.setLevel(level);
  }

  @Override
  public NodeQuery setRange(final int lowerBound, final int upperBound) {
    return (NodeQuery) super.setRange(lowerBound, upperBound);
  }

  @Override
  public NodeQuery setBoost(final float boost) {
    return (NodeQuery) super.setBoost(boost);
  }

  @Override
  public Query toQuery(final boolean proxy) throws QueryNodeException {
    final com.sindicetech.siren.search.node.NodeQuery query = (com.sindicetech.siren.search.node.NodeQuery) parser.parse(booleanExpression, "");
    if (this.hasLevel()) {
      query.setLevelConstraint(this.getLevel());
    }
    if (this.hasRange()) {
      query.setNodeConstraint(this.getLowerBound(), this.getUpperBound());
    }
    if (this.hasBoost()) {
      query.setBoost(this.getBoost());
    }

    // should we wrap the query into a lucene proxy
    if (proxy) {
      return new LuceneProxyNodeQuery(query);
    }
    return query;
  }

  @Override
  public ObjectNode toJson() {
    final ObjectNode obj = mapper.createObjectNode();
    final ObjectNode node = obj.putObject(NodePropertyParser.NODE_PROPERTY);
    node.put(QueryPropertyParser.QUERY_PROPERTY, booleanExpression);
    if (this.hasLevel()) {
      node.put(LevelPropertyParser.LEVEL_PROPERTY, this.getLevel());
    }
    if (this.hasRange()) {
      final ArrayNode array = node.putArray(RangePropertyParser.RANGE_PROPERTY);
      array.add(this.getLowerBound());
      array.add(this.getUpperBound());
    }
    if (this.hasBoost()) {
      node.put(BoostPropertyParser.BOOST_PROPERTY, this.getBoost());
    }
    return obj;
  }

}
