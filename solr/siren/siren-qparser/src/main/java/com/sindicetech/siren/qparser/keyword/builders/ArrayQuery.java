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
package com.sindicetech.siren.qparser.keyword.builders;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;

import com.sindicetech.siren.qparser.keyword.nodes.ArrayQueryNode;

/**
 * An {@link ArrayQuery} is a {@link Query} object that is used to store
 * a list of queries.
 *
 * @see ArrayQueryNode
 */
final class ArrayQuery
extends Query {

  private final ArrayList<Query> elements = new ArrayList<Query>();

  /**
   * Add a {@link Query} to the array
   */
  public void addElement(final Query q) {
    elements.add(q);
  }

  /**
   * Returns the list of queries.
   */
  public List<Query> getElements() {
    return elements;
  }

  @Override
  public String toString(final String field) {
    final StringBuilder sb = new StringBuilder("[ ");

    for (int i = 0; i < elements.size(); i++) {
      sb.append(elements.get(i).toString(field));
      if (i + 1 != elements.size()) {
        sb.append(", ");
      }
    }
    sb.append(" ]");
    return sb.toString();
  }

}
