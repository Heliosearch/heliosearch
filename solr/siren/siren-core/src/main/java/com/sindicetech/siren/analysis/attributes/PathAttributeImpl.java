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

import org.apache.lucene.util.AttributeImpl;

import java.io.Serializable;

/**
 * Default implementation of {@link PathAttribute}.
 */
public class PathAttributeImpl extends AttributeImpl
implements PathAttribute, Cloneable, Serializable {

  private String[] path = new String[0];

  private static final long serialVersionUID = -6117733199775936595L;

  @Override
  public String field() {
    return path.length > 0 ? path[path.length - 1] : "";
  }

  @Override
  public String[] path() {
    return path;
  }

  @Override
  public void setPath(final String[] path) {
    this.path = path;
  }

  @Override
  public void clear() {
    path = new String[0];
  }

  @Override
  public void copyTo(final AttributeImpl target) {
    final PathAttributeImpl t = (PathAttributeImpl) target;
    t.clear();
    t.setPath(path);
  }

}
