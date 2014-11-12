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

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.codehaus.jackson.JsonNode;

import com.sindicetech.siren.qparser.tree.ParseException;
import com.sindicetech.siren.qparser.tree.nodes.ArrayQueryNode;

import java.util.Iterator;

/**
 * Parses a clause and returns a {@link com.sindicetech.siren.qparser.tree.nodes.ArrayQueryNode}.
 * <p>
 * A clause is composed of:
 * <ul>
 *   <li> an attribute {@link com.sindicetech.siren.qparser.tree.parser.OccurPropertyParser#OCCUR_PROPERTY}
 *   <li> one of the four attributes: {@link NodePropertyParser#NODE_PROPERTY},
 *        {@link TwigPropertyParser#TWIG_PROPERTY}, {@link BooleanPropertyParser#BOOLEAN_PROPERTY},
 *        {@link VariablePropertyParser#VARIABLE_PROPERTY}
 * </ul>
 */
public abstract class AbstractClausePropertyParser extends JsonPropertyParser {

  public AbstractClausePropertyParser(final JsonNode node, final CharSequence field) {
    super(node, field);
  }

  protected abstract ModifierQueryNode newClauseQueryNode(QueryNode node, Modifier mod);

  protected abstract ModifierQueryNode parseClauseOptions(final JsonNode element, ModifierQueryNode node) throws ParseException;

  @Override
  ArrayQueryNode parse() throws ParseException {
    final JsonNode value = node.path(this.getProperty());
    if (!value.isArray()) {
      throw new ParseException("Invalid property '" + this.getProperty() + "': value is not an array");
    }

    final ArrayQueryNode arrayNode = new ArrayQueryNode();

    final Iterator<JsonNode> elements = value.getElements();
    while (elements.hasNext()) {
      final JsonNode element = elements.next();

      // parse occur: optional, if not defined, we use the default modifier
      final OccurPropertyParser occurParser = new OccurPropertyParser(element, field);
      occurParser.setOptional(true);
      Modifier mod = this.getDefaultModifier();
      if (occurParser.isPropertyDefined()) {
        mod = occurParser.parse();
      }

      // check if there is either a node, a twig, a boolean or a variable property and parse it
      QueryNode queryNode = null;
      if (element.has(NodePropertyParser.NODE_PROPERTY)) {
        final NodePropertyParser nodeParser = new NodePropertyParser(element, field);
        queryNode = nodeParser.parse();
      }
      else if (element.has(TwigPropertyParser.TWIG_PROPERTY)) {
        final TwigPropertyParser twigParser = new TwigPropertyParser(element, field);
        twigParser.setDefaultModifier(this.getDefaultModifier());
        queryNode = twigParser.parse();
      }
      else if (element.has(BooleanPropertyParser.BOOLEAN_PROPERTY)) {
        final BooleanPropertyParser booleanParser = new BooleanPropertyParser(element, field);
        booleanParser.setDefaultModifier(this.getDefaultModifier());
        queryNode = booleanParser.parse();
      }
      else if (element.has(VariablePropertyParser.VARIABLE_PROPERTY)) {
        final VariablePropertyParser variableParser = new VariablePropertyParser(element, field);
        queryNode = variableParser.parse();
      }

      // check if either a node, a twig, a boolean or a variable property has been defined
      if (queryNode == null) {
        throw new ParseException("Invalid property '" + this.getProperty() + "': object does not define a boolean, " +
        "twig or node query");
      }

      // wrap the query node with a modifier
      ModifierQueryNode modifierQueryNode = this.newClauseQueryNode(queryNode, mod);

      // parse the additional options of the clause object
      modifierQueryNode = this.parseClauseOptions(element, modifierQueryNode);

      // add it to the list of clauses
      arrayNode.add(modifierQueryNode);
    }

    return arrayNode;
  }

}
