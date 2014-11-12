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
package com.sindicetech.siren.qparser.tree.parser;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.sindicetech.siren.qparser.tree.ParseException;
import com.sindicetech.siren.qparser.tree.nodes.TopLevelQueryNode;

import java.io.IOException;
import java.util.Iterator;

/**
 * Parser for the SIREN's JSON query syntax
 *
 * <p>
 *
 * This parser is based on the Jackson's JSON parser and uses internally
 * an {@link ObjectMapper} to create a tree model of the JSON data. The
 * tree model is then traversed using a property-centric visitor model.
 * The parser works as follows:
 * <ol>
 *   <li> The JSON data is parsed and a JSON tree model is created.
 *   <li> The tree model is then traversed using {@link JsonPropertyParser}s.
 *   For each property found in the tree model, the corresponding
 *   {@link JsonPropertyParser} is applied.
 * </ol>
 */
public class JsonSyntaxParser implements SyntaxParser {

  private final ObjectMapper mapper;

  private ModifierQueryNode.Modifier defaultModifier;

  public JsonSyntaxParser() {
    this.mapper = new ObjectMapper();
  }

  @Override
  public QueryNode parse(final CharSequence query, final CharSequence field)
  throws QueryNodeParseException {
    try {
      final JsonNode node = mapper.readTree(query.toString());
      final String fieldname = this.getFirstFieldName(node);
      final TopLevelQueryNode topNode = new TopLevelQueryNode();

      // check for node property
      if (fieldname.equals(NodePropertyParser.NODE_PROPERTY)) {
        final NodePropertyParser nodeParser = new NodePropertyParser(node, field);
        topNode.add(nodeParser.parse());
        return topNode;
      }
      // check for twig property
      if (fieldname.equals(TwigPropertyParser.TWIG_PROPERTY)) {
        final TwigPropertyParser twigParser = new TwigPropertyParser(node, field);
        twigParser.setDefaultModifier(this.defaultModifier);
        topNode.add(twigParser.parse());
        return topNode;
      }
      // check for boolean property
      if (fieldname.equals(BooleanPropertyParser.BOOLEAN_PROPERTY)) {
        final BooleanPropertyParser booleanParser = new BooleanPropertyParser(node, field);
        booleanParser.setDefaultModifier(this.defaultModifier);
        topNode.add(booleanParser.parse());
        return topNode;
      }
      throw new ParseException("Invalid JSON query: unknown property '" + fieldname + "'");
    }
    catch (final IOException e) {
      throw new ParseException("Invalid JSON query", e);
    }

  }

  public void setDefaultModifier(final ModifierQueryNode.Modifier operator) {
    this.defaultModifier = operator;
  }

  private String getFirstFieldName(final JsonNode node) throws ParseException {
    final Iterator<String> fieldNames = node.getFieldNames();
    if (fieldNames.hasNext()) {
      return fieldNames.next();
    }
    throw new ParseException("Invalid JSON query: either a node, boolean or twig query must be defined");
  }

}
