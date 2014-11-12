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

package com.sindicetech.siren.qparser.keyword.processors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler.KeywordConfigurationKeys;
import com.sindicetech.siren.qparser.keyword.nodes.DatatypeQueryNode;
import com.sindicetech.siren.qparser.keyword.nodes.TwigQueryNode;
import com.sindicetech.siren.util.JSONDatatype;
import com.sindicetech.siren.util.XSDDatatype;

import java.util.List;
import java.util.Map;

/**
 * This processor tags at pre-processing all the {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}s
 * of the query tree with the datatype label using the TAG {@link DatatypeQueryNode#DATATYPE_TAGID}.
 *
 * <p>
 *
 * If it encounters a {@link DatatypeQueryNode}, it will tag all its descendant with the provided datatype label.
 *
 * <p>
 *
 * By default, a node without a {@link DatatypeQueryNode} ancestor is tagged
 * with {@link XSDDatatype#XSD_STRING}.
 *
 * <p>
 *
 * The top level node of a twig is tagged with {@link JSONDatatype#JSON_FIELD}.
 * If a custom datatype is used on the top level node, it is used instead of
 * {@link JSONDatatype#JSON_FIELD}.
 *
 * <p>
 *
 * At post-processing, this processor removes the {@link DatatypeQueryNode}.
 */
public class DatatypeProcessor extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    final QueryConfigHandler conf = this.getQueryConfigHandler();

    // If the current node is a datatype query node, validate the datatype and assign it to its child
    if (node instanceof DatatypeQueryNode) {
      final Map<String, Analyzer> dtAnalyzers = conf.get(KeywordConfigurationKeys.DATATYPES_ANALYZERS);
      final DatatypeQueryNode dt = (DatatypeQueryNode) node;
      String datatype = dt.getDatatype();

      // check if the datatype is correctly registered
      if (dtAnalyzers == null) {
        throw new IllegalArgumentException("KeywordConfigurationKeys.DATAYPES_ANALYZERS " +
            "should be set on the ExtendedKeywordQueryConfigHandler");
      }
      if (!dtAnalyzers.containsKey(datatype)) {
        throw new IllegalArgumentException("Unknown datatype: [" + datatype + "]");
      }
      if (dtAnalyzers.get(datatype) == null) {
        throw new IllegalArgumentException("Analyzer of datatype [" + datatype + "] cannot be null.");
      }

      // transfer the datatype to its child
      dt.getChild().setTag(DatatypeQueryNode.DATATYPE_TAGID, datatype);
    }
    // If the current node is a twig query node, assign the json:field datatype to the root, and assign the default
    // or the tagged datatype to the child
    else if (node instanceof TwigQueryNode) {
      final TwigQueryNode twig = (TwigQueryNode) node;
      if (twig.getTag(DatatypeQueryNode.DATATYPE_TAGID) == null) {
        twig.getChild().setTag(DatatypeQueryNode.DATATYPE_TAGID, this.getDefaultDatatype(conf));
      }
      else {
        twig.getChild().setTag(DatatypeQueryNode.DATATYPE_TAGID, this.getDatatype(conf, node));
      }
      twig.getRoot().setTag(DatatypeQueryNode.DATATYPE_TAGID, JSONDatatype.JSON_FIELD);
    }
    // in any other cases, if the node is not a leaf node, transfer the datatype to its children
    else if (!node.isLeaf()) {
      for (final QueryNode child : node.getChildren()) {
        child.setTag(DatatypeQueryNode.DATATYPE_TAGID, this.getDatatype(conf, node));
      }
    }

    return node;
  }

  @Override
  protected QueryNode postProcessNode(final QueryNode node) throws QueryNodeException {
    // we have processed the DatatypeQueryNode, we can remove it now
    if (node instanceof DatatypeQueryNode) {
      final DatatypeQueryNode dt = (DatatypeQueryNode) node;
      // return its child
      return dt.getChild();
    }
    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(final List<QueryNode> children) throws QueryNodeException {
    return children;
  }

  /**
   * Return the datatype label associated with the given node. If there is no datatype label associated, returns the
   * default datatype.
   */
  static String getDatatype(QueryConfigHandler config, QueryNode node) {
    // if a datatype has been defined, return it
    if (node.getTag(DatatypeQueryNode.DATATYPE_TAGID) != null) {
      return (String) node.getTag(DatatypeQueryNode.DATATYPE_TAGID);
    }
    // otherwise, return the default datatype
    else {
      return getDefaultDatatype(config);
    }
  }

  public static String getDefaultDatatype(QueryConfigHandler config) {
    if (config.has(KeywordConfigurationKeys.DEFAULT_DATATYPE)) {
      return config.get(KeywordConfigurationKeys.DEFAULT_DATATYPE);
    }
    else {
      throw new IllegalArgumentException("KeywordConfigurationKeys.DEFAULT_DATATYPE should be set on the ExtendedKeywordQueryConfigHandler");
    }
  }

}
