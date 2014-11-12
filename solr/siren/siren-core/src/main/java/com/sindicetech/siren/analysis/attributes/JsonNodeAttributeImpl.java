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
package com.sindicetech.siren.analysis.attributes;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.IntsRef;

import com.sindicetech.siren.util.ArrayUtils;

/**
 * Implementation of {@link NodeAttribute} for a token coming from the
 * {@link com.sindicetech.siren.analysis.ExtendedJsonTokenizer}.
 */
public class JsonNodeAttributeImpl
extends AttributeImpl
implements NodeAttribute, Cloneable, Serializable {

  private static final long serialVersionUID = 8820316999175774635L;
  private final IntsRef     node             = new IntsRef();

  /**
   * Returns this Token's node path.
   */
  public IntsRef node() {
    return node;
  }

  @Override
  public void clear() {
    node.length = 0;
    node.offset = 0;
    Arrays.fill(node.ints, 0);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    }

    if (other instanceof JsonNodeAttributeImpl) {
      final JsonNodeAttributeImpl o = (JsonNodeAttributeImpl) other;
      return node.equals(o.node);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return node.hashCode();
  }

  @Override
  public void copyTo(final AttributeImpl target) {
    final JsonNodeAttributeImpl t = (JsonNodeAttributeImpl) target;
    t.copyNode(node);
  }

  @Override
  public void copyNode(final IntsRef nodePath) {
    ArrayUtils.growAndCopy(node, nodePath.length);
    System.arraycopy(nodePath.ints, nodePath.offset, node.ints, node.offset, nodePath.length);
    node.offset = nodePath.offset;
    node.length = nodePath.length;
  }

  @Override
  public String toString() {
    return "node=" + this.node();
  }

  @Override
  public void append(final int nodeID) {
    ArrayUtils.growAndCopy(node, node.length + 1);
    node.ints[++node.length] = nodeID;
  }

}
