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

package com.sindicetech.siren.qparser.keyword.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;

/**
 * This {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode} represents an empty
 * {@link org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode}. This is created when the query string is
 * empty. It will be converted into a {@link org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode}.
 */
public class EmptyFieldQueryNode extends FieldQueryNode {


  public EmptyFieldQueryNode() {
    super("", "", 0, 0);
  }

}
