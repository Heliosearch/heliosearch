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

package com.sindicetech.siren.index;

import java.security.InvalidParameterException;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.MultiDocsAndPositionsEnum;

import com.sindicetech.siren.search.node.NodeQuery;

/**
 * This {@link DocsAndPositionsEnum} extension acts as a decorator over a
 * {@link DocsNodesAndPositionsEnum}.
 *
 * <p>
 *
 * It enables to provide a {@link DocsNodesAndPositionsEnum} instance to a
 * {@link NodeQuery}.
 *
 * <p>
 *
 * Subclasses must wrap a {@link DocsNodesAndPositionsEnum} instance and
 * implements the method {@link SirenDocsEnum#getDocsNodesAndPositionsEnum()}
 * to return the wrapped instance.
 */
public abstract class SirenDocsEnum extends DocsAndPositionsEnum {

  public abstract DocsNodesAndPositionsEnum getDocsNodesAndPositionsEnum();

  /**
   * Helper method to map a Lucene's {@link DocsAndPositionsEnum} to a SIREn's
   * {@link DocsNodesAndPositionsEnum}.
   */
  public static DocsNodesAndPositionsEnum map(final DocsAndPositionsEnum docsEnum) {
    if (docsEnum instanceof MultiDocsAndPositionsEnum) {
      final MultiDocsAndPositionsEnum multiDocsEnum = (MultiDocsAndPositionsEnum) docsEnum;
      return new MultiDocsNodesAndPositionsEnum(multiDocsEnum.getSubs(), multiDocsEnum.getNumSubs());
    }
    else if (docsEnum instanceof SirenDocsEnum) {
      return ((SirenDocsEnum) docsEnum).getDocsNodesAndPositionsEnum();
    }
    else {
      throw new InvalidParameterException("Unknown DocsAndPositionsEnum received: " + docsEnum.getClass());
    }
  }

}
