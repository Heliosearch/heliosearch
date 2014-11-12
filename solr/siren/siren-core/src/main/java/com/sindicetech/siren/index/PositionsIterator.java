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

import java.io.IOException;

/**
 * This interface defines methods to iterate over a set of increasing
 * positions.
 *
 * <p>
 *
 * This class iterates on positions. The sentinel value
 * {@link #NO_MORE_POS} (which is set to {@value #NO_MORE_POS}) is used to
 * indicate the end of the position stream.
 *
 * <p>
 *
 * To be used in conjunction with {@link DocsAndNodesIterator}.
 */
public interface PositionsIterator  {

  /**
   * When returned by {@link #pos()} it means there are no more
   * positions in the iterator.
   */
  public static final int NO_MORE_POS = Integer.MAX_VALUE;

  /**
   * Move to the next position in the current node matching the query.
   * <p>
   * Should not be called until {@link DocsAndNodesIterator#nextNode()} is called for the first
   * time.
   *
   * @return false if there is no more position for the current node or if
   * {@link DocsAndNodesIterator#nextNode()} was not called yet.
   */
  public boolean nextPosition() throws IOException;

  /**
   * Returns the following:
   * <ul>
   * <li>-1 or {@link #NO_MORE_POS} if {@link #nextPosition()} was not called
   * yet.
   * <li>{@link #NO_MORE_POS} if the iterator has exhausted.
   * <li>Otherwise it should return the position it is currently on.
   * </ul>
   */
  public int pos();

}
