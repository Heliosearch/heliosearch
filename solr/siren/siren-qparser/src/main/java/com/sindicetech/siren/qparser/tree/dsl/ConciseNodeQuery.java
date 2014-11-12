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

import com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser;
import com.sindicetech.siren.qparser.tree.parser.AttributePropertyParser;
import com.sindicetech.siren.qparser.tree.parser.NodePropertyParser;
import com.sindicetech.siren.search.node.LuceneProxyNodeQuery;

/**
 * Extension of the {@link NodeQuery} for the concise model. It adds the support for the attribute concept.
 */
public class ConciseNodeQuery extends NodeQuery {

  /**
   * The attribute assigned to this node query. Only in concise model.
   */
  private String attribute;
  private boolean hasAttribute = false;

  ConciseNodeQuery(final ObjectMapper mapper, final ConciseKeywordQueryParser parser) {
    super(mapper, parser);
  }

  public ConciseNodeQuery setAttribute(final String attribute) {
    this.attribute = attribute;
    this.hasAttribute = true;
    return this;
  }

  @Override
  public Query toQuery(final boolean proxy) throws QueryNodeException {
    // ensure that the attribute is unset
    ((ConciseKeywordQueryParser) parser).unsetAttribute();

    // set attribute
    if (this.hasAttribute) {
      ((ConciseKeywordQueryParser) parser).setAttribute(attribute);
    }

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
    ObjectNode obj = super.toJson();
    if (this.hasAttribute) {
      ObjectNode node = (ObjectNode) obj.get(NodePropertyParser.NODE_PROPERTY);
      node.put(AttributePropertyParser.ATTRIBUTE_PROPERTY, attribute);
    }
    return obj;
  }

}
