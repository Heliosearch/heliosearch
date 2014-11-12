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
package com.sindicetech.siren.search.node;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 * A simple {@link NodeQuery} that matches nodes containing a variable.
 * 
 * It is rewritten to an empty {@link NodeBooleanQuery} which doesn't 
 * change evaluation of the rest of the query.
 *
 */
public class NodeVariableQuery extends NodeQuery {

    @Override
    public String toString(String arg0) {
        return "variable";
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return new NodeBooleanQuery();
    }
}
