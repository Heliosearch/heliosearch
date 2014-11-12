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
package com.sindicetech.siren.qparser.tree;

import com.sindicetech.siren.qparser.tree.builders.concise.ConciseTreeQueryTreeBuilder;
import com.sindicetech.siren.qparser.tree.config.ConciseTreeQueryConfigHandler;
import com.sindicetech.siren.qparser.tree.parser.JsonSyntaxParser;
import com.sindicetech.siren.qparser.tree.processors.JsonQueryNodeProcessorPipeline;

/**
 * An extension of the {@link ExtendedTreeQueryParser} for the concise model. It introduces
 * the <code>attribute</code> property for <code>node</code> object, and modifies the conversion of
 * {@link com.sindicetech.siren.qparser.tree.nodes.NodeQueryNode} and
 * {@link com.sindicetech.siren.qparser.tree.nodes.TwigQueryNode}.
 *
 * <p>
 *
 * To assign an attribute to a node query, one can use the <code>attribute</code> property in a <code>node</code>
 * object:
 * <pre>
 * {
 *   "node" : {
 *     "attribute" : "age",
 *     "query" : "32"
 *   }
 * }
 * </pre>
 * This will be converted into a {@link com.sindicetech.siren.search.node.NodeTermQuery} with <code>age:32</code> as
 * query term.
 *
 * <p>
 *
 * The query expression from a <code>root</code> property of a <code>twig</code> object will be mapped to an attribute
 * label. In other words, the following twig object:
 * <pre>
 * {
 *   "twig" : {
 *     "root" : "age"
 *   }
 * }
 * </pre>
 * will be converted into a {@link com.sindicetech.siren.search.node.TwigQuery} with a root clause being a
 * {@link com.sindicetech.siren.search.node.NodeTermQuery} with <code>age:</code> as query term.
 *
 * <p>
 *
 * The builder used by this helper is a {@link com.sindicetech.siren.qparser.tree.builders.concise.ConciseTreeQueryTreeBuilder}.
 *
 * @see ExtendedTreeQueryParser
 * @see com.sindicetech.siren.qparser.tree.config.ConciseTreeQueryConfigHandler
 * @see com.sindicetech.siren.qparser.tree.builders.concise.ConciseTreeQueryTreeBuilder
 */
public class ConciseTreeQueryParser extends ExtendedTreeQueryParser {

  public ConciseTreeQueryParser() {
    super(new ConciseTreeQueryConfigHandler(), new JsonSyntaxParser(),
      new JsonQueryNodeProcessorPipeline(null),
      new ConciseTreeQueryTreeBuilder(null));
  }

}
