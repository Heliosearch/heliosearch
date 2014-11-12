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

import org.apache.lucene.util.AttributeImpl;

/**
 * Default implementation of {@link DatatypeAttribute}.
 */
public class DatatypeAttributeImpl extends AttributeImpl
implements DatatypeAttribute, Cloneable, Serializable {

  private char[] dataTypeURI = null;

  private static final long serialVersionUID = -6117733199775936595L;

  @Override
  public char[] datatypeURI() {
    return dataTypeURI;
  }

  @Override
  public void setDatatypeURI(final char[] datatypeURI) {
    this.dataTypeURI = datatypeURI;
  }

  @Override
  public void clear() {
    dataTypeURI = null;
  }

  @Override
  public void copyTo(final AttributeImpl target) {
    final DatatypeAttributeImpl t = (DatatypeAttributeImpl) target;
    t.clear();
    t.setDatatypeURI(dataTypeURI);
  }

}
