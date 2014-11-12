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

import com.sindicetech.siren.qparser.tree.builders.BooleanQueryNodeBuilder;
import com.sindicetech.siren.qparser.tree.parser.*;
import com.sindicetech.siren.search.node.*;
import com.sindicetech.siren.search.spans.BooleanSpanQuery;
import com.sindicetech.siren.search.spans.NodeSpanQuery;
import com.sindicetech.siren.search.spans.SpanQuery;

/**
 * Class that represents a boolean query or a boolean span query object of the JSON query syntax.
 */
public class BooleanQuery extends AbstractBooleanQuery {

  BooleanQuery(final ObjectMapper mapper) {
    super(mapper);
  }

  @Override
  public Query toQuery(final boolean proxy) throws QueryNodeException {
    Query query;

    // boolean span query
    if (this.hasSlop() || this.hasInOrder()) {
      query = this.toBooleanSpanQuery();
      // should we wrap the query into a lucene proxy
      if (proxy) {
        query = new LuceneProxyNodeQuery((com.sindicetech.siren.search.node.NodeQuery) query);
      }
    }
    // lucene boolean query
    else {
      query = this.toBooleanQuery();
    }

    // add boost
    if (this.hasBoost()) {
      query.setBoost(this.getBoost());
    }

    return query;
  }

  private BooleanSpanQuery toBooleanSpanQuery() throws QueryNodeException {
    int slop = this.hasSlop() ? this.getSlop() : BooleanQueryNodeBuilder.DEFAULT_SLOP;
    boolean inOrder = this.hasInOrder() ? this.getInOrder() : BooleanQueryNodeBuilder.DEFAULT_INORDER;
    final BooleanSpanQuery query = new BooleanSpanQuery(slop, inOrder);
    // convert clauses
    for (final QueryClause clause : clauses) {
      final SpanQuery q = new NodeSpanQuery((com.sindicetech.siren.search.node.NodeQuery) clause.getQuery().toQuery(false));
      query.add(q, clause.getNodeBooleanOccur());
    }
    return query;
  }

  private org.apache.lucene.search.BooleanQuery toBooleanQuery() throws QueryNodeException {
    final org.apache.lucene.search.BooleanQuery query = new org.apache.lucene.search.BooleanQuery(true);
    // convert clauses
    for (final QueryClause clause : clauses) {
      Query q = clause.getQuery().toQuery(false);
      if (q instanceof com.sindicetech.siren.search.node.NodeQuery) {
        // wrap the query into a LuceneProxyNodeQuery
        q = new LuceneProxyNodeQuery((com.sindicetech.siren.search.node.NodeQuery) q);
      }
      query.add(q, clause.getBooleanOccur());
    }
    return query;
  }

  @Override
  public ObjectNode toJson() {
    final ObjectNode obj = mapper.createObjectNode();
    ObjectNode bool = obj.putObject(BooleanPropertyParser.BOOLEAN_PROPERTY);

    // add boost
    if (this.hasBoost()) {
      bool.put(BoostPropertyParser.BOOST_PROPERTY, this.getBoost());
    }

    // add slop
    if (this.hasSlop()) {
      bool.put(SlopPropertyParser.SLOP_PROPERTY, this.getSlop());
    }

    // add inOrder
    if (this.hasInOrder()) {
      bool.put(InOrderPropertyParser.IN_ORDER_PROPERTY, this.getInOrder());
    }

    // add clauses
    ArrayNode array = bool.putArray(ClausePropertyParser.CLAUSE_PROPERTY);
    for (final QueryClause clause : clauses) {
      final ObjectNode e = array.addObject();
      e.put(OccurPropertyParser.OCCUR_PROPERTY, clause.getOccur().toString());
      e.putAll(clause.getQuery().toJson());
    }

    return obj;
  }


}
