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

import org.apache.lucene.search.Weight;
import org.apache.lucene.util.IntsRef;

/**
 * An empty implementation of a {@link NodeScorer} for variables.
 * 
 * Should never be used, all methods except constructor throw an
 * {@link UnsupportedOperationException}.
 */
public class NodeVariableScorer extends NodeScorer {

    protected NodeVariableScorer(Weight weight) {
        super(weight);
    }
    
    private void throwException() {
        throw new UnsupportedOperationException("Not supported for NodeVariable.");
    }

    @Override
    public boolean nextCandidateDocument() throws IOException {
        throwException();
        return false;
    }

    @Override
    public boolean nextNode() throws IOException {
        throwException();
        return false;
    }

    @Override
    public boolean skipToCandidate(int target) throws IOException {
        throwException();
        return false;
    }

    @Override
    public int doc() {
        throwException();
        return 0;
    }

    @Override
    public IntsRef node() {
        throwException();
        return null;
    }

    @Override
    public int freqInNode() throws IOException {
        throwException();
        return 0;
    }

    @Override
    public float scoreInNode() throws IOException {
        throwException();
        return 0;
    }

}
