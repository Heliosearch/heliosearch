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
package com.sindicetech.siren.analysis;

import org.apache.lucene.util.IntsRef;

import java.util.*;

public class MockSirenDocument {

  TreeMap<IntsRef, ArrayList<MockSirenToken>> sortedTokens;

  private final Comparator<IntsRef> INTS_COMP = new Comparator<IntsRef>() {

    public int compare(final IntsRef ints1, final IntsRef ints2) {
        return ints1.compareTo(ints2);
    }

  };

  public MockSirenDocument(final MockSirenToken ... tokens) {
    sortedTokens = new TreeMap<IntsRef, ArrayList<MockSirenToken>>(INTS_COMP);

    IntsRef ints;
    for (final MockSirenToken token : tokens) {
      ints = token.nodePath;
      if (!sortedTokens.containsKey(ints)) {
        sortedTokens.put(ints, new ArrayList<MockSirenToken>());
      }
      sortedTokens.get(ints).add(token);
    }
  }

  public Iterator<ArrayList<MockSirenToken>> iterator() {
    return sortedTokens.values().iterator();
  }

  public static MockSirenDocument doc(final MockSirenToken ... tokens) {
    return new MockSirenDocument(tokens);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ ");
    for (Map.Entry<IntsRef, ArrayList<MockSirenToken>> entry : sortedTokens.entrySet()) {
      builder.append(entry.getKey());
      builder.append(" :(");
      for (MockSirenToken token : entry.getValue()) {
        builder.append(" ");
        builder.append(token);
      }
      builder.append(" )");
    }
    builder.append(" }");
    return builder.toString();
  }

}

