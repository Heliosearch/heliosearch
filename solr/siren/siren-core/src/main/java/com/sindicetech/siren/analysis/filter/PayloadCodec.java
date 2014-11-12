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

package com.sindicetech.siren.analysis.filter;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/**
 * Abstract class implementation of the SIREn payload coder/decoder.
 *
 * <p>
 *
 * The SIREn payload stores information about a token such as:
 * <ul>
 * <li> the dewey code of the node from which this token comes from;
 * <li> the relative position of the token within the node.
 * </ul>
 */
public abstract class PayloadCodec {

  public PayloadCodec() {}

  public abstract IntsRef getNode();

  public abstract int getPosition();

  /**
   * Encode the information into a byte array
   */
  public abstract BytesRef encode(IntsRef node, int pos);

  /**
   * Decode the node and position from the byte array
   */
  public abstract void decode(BytesRef data);

}
